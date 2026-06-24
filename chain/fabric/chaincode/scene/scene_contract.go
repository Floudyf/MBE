package main

import (
	"encoding/json"
	"fmt"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

type SceneContract struct{ contractapi.Contract }

func sceneKey(id string) string                    { return "scene:" + id }
func avatarKey(id string) string                   { return "avatar:" + id }
func sceneMemberKey(sceneID, userID string) string { return "scene_member:" + sceneID + ":" + userID }

// JoinScene rejects duplicate joins so that MemberCount remains a count of unique members.
func (c *SceneContract) JoinScene(ctx contractapi.TransactionContextInterface, userID, sceneID string) error {
	scene, err := c.ReadScene(ctx, sceneID)
	if err != nil {
		return err
	}
	if _, err = c.ReadAvatar(ctx, userID); err != nil {
		return err
	}
	memberKey := sceneMemberKey(sceneID, userID)
	existing, err := ctx.GetStub().GetState(memberKey)
	if err != nil {
		return err
	}
	if existing != nil {
		return fmt.Errorf("avatar already joined scene")
	}
	if scene.Capacity > 0 && scene.MemberCount >= scene.Capacity {
		return fmt.Errorf("scene capacity reached")
	}
	member, err := json.Marshal(SceneMember{SceneID: sceneID, UserID: userID})
	if err != nil {
		return err
	}
	if err = ctx.GetStub().PutState(memberKey, member); err != nil {
		return err
	}
	scene.MemberCount++
	scene.Version++
	encodedScene, err := json.Marshal(scene)
	if err != nil {
		return err
	}
	if err = ctx.GetStub().PutState(sceneKey(sceneID), encodedScene); err != nil {
		return err
	}
	p := map[string]any{"tx_type": "scene_join", "contract": "scene", "function": "JoinScene", "args": map[string]string{"user_id": userID, "scene_id": sceneID}, "primary_key": sceneKey(sceneID), "access_keys": []string{sceneKey(sceneID), avatarKey(userID), memberKey}, "event": "SceneJoined"}
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return ctx.GetStub().SetEvent("SceneJoined", b)
}

func (c *SceneContract) CreateScene(ctx contractapi.TransactionContextInterface, id, name string, capacity int) error {
	existing, err := ctx.GetStub().GetState(sceneKey(id))
	if err != nil {
		return err
	}
	if existing != nil {
		return fmt.Errorf("scene exists")
	}
	b, err := json.Marshal(Scene{ID: id, Name: name, Capacity: capacity, Version: 1})
	if err != nil {
		return err
	}
	return ctx.GetStub().PutState(sceneKey(id), b)
}

func (c *SceneContract) CreateAvatar(ctx contractapi.TransactionContextInterface, id, status string) error {
	existing, err := ctx.GetStub().GetState(avatarKey(id))
	if err != nil {
		return err
	}
	if existing != nil {
		return fmt.Errorf("avatar exists")
	}
	b, err := json.Marshal(Avatar{UserID: id, Status: status})
	if err != nil {
		return err
	}
	return ctx.GetStub().PutState(avatarKey(id), b)
}

func (c *SceneContract) ReadScene(ctx contractapi.TransactionContextInterface, id string) (*Scene, error) {
	b, err := ctx.GetStub().GetState(sceneKey(id))
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, fmt.Errorf("scene missing")
	}
	var scene Scene
	if err = json.Unmarshal(b, &scene); err != nil {
		return nil, err
	}
	return &scene, nil
}

func (c *SceneContract) ReadAvatar(ctx contractapi.TransactionContextInterface, id string) (*Avatar, error) {
	b, err := ctx.GetStub().GetState(avatarKey(id))
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, fmt.Errorf("avatar missing")
	}
	var avatar Avatar
	if err = json.Unmarshal(b, &avatar); err != nil {
		return nil, err
	}
	return &avatar, nil
}

func (c *SceneContract) InitLedger(ctx contractapi.TransactionContextInterface) error { return nil }

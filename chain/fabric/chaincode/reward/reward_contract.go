package main

import (
	"encoding/json"
	"fmt"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

type RewardContract struct{ contractapi.Contract }

func rewardPoolKey(id string) string { return "reward_pool:" + id }
func balanceKey(id string) string    { return "balance:" + id }

func (c *RewardContract) AddReward(ctx contractapi.TransactionContextInterface, poolID string, amount int) error {
	if amount < 0 {
		return fmt.Errorf("reward amount must be non-negative")
	}
	pool, err := c.ReadRewardPool(ctx, poolID)
	if err != nil {
		return err
	}
	pool.Amount += amount
	pool.Version++
	b, err := json.Marshal(pool)
	if err != nil {
		return err
	}
	if err = ctx.GetStub().PutState(rewardPoolKey(poolID), b); err != nil {
		return err
	}
	return c.emit(ctx, "RewardAdded", "add_reward", "AddReward", poolID, "", amount)
}

func (c *RewardContract) ClaimReward(ctx contractapi.TransactionContextInterface, userID, poolID string, amount int) error {
	if amount < 0 {
		return fmt.Errorf("claim amount must be non-negative")
	}
	pool, err := c.ReadRewardPool(ctx, poolID)
	if err != nil {
		return err
	}
	if pool.Amount < amount {
		return fmt.Errorf("insufficient reward pool balance")
	}
	balance, err := c.GetBalance(ctx, userID)
	if err != nil {
		return err
	}
	pool.Amount -= amount
	pool.Version++
	balance.Amount += amount
	poolBytes, err := json.Marshal(pool)
	if err != nil {
		return err
	}
	balanceBytes, err := json.Marshal(balance)
	if err != nil {
		return err
	}
	if err = ctx.GetStub().PutState(rewardPoolKey(poolID), poolBytes); err != nil {
		return err
	}
	if err = ctx.GetStub().PutState(balanceKey(userID), balanceBytes); err != nil {
		return err
	}
	return c.emit(ctx, "RewardClaimed", "reward_claim", "ClaimReward", poolID, userID, -amount)
}

func (c *RewardContract) emit(ctx contractapi.TransactionContextInterface, event, typ, fn, pool, user string, delta int) error {
	p := map[string]any{"tx_type": typ, "contract": "reward", "function": fn, "args": map[string]any{"pool_id": pool, "user_id": user, "amount": abs(delta)}, "primary_key": rewardPoolKey(pool), "access_keys": []string{rewardPoolKey(pool), balanceKey(user)}, "delta_value": delta, "event": event}
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return ctx.GetStub().SetEvent(event, b)
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func (c *RewardContract) CreateRewardPool(ctx contractapi.TransactionContextInterface, id string, amount int) error {
	if amount < 0 {
		return fmt.Errorf("reward amount must be non-negative")
	}
	existing, err := ctx.GetStub().GetState(rewardPoolKey(id))
	if err != nil {
		return err
	}
	if existing != nil {
		return fmt.Errorf("reward pool exists")
	}
	b, err := json.Marshal(RewardPool{ID: id, Amount: amount, Version: 1})
	if err != nil {
		return err
	}
	return ctx.GetStub().PutState(rewardPoolKey(id), b)
}

func (c *RewardContract) ReadRewardPool(ctx contractapi.TransactionContextInterface, id string) (*RewardPool, error) {
	b, err := ctx.GetStub().GetState(rewardPoolKey(id))
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, fmt.Errorf("reward pool missing")
	}
	var pool RewardPool
	if err = json.Unmarshal(b, &pool); err != nil {
		return nil, err
	}
	return &pool, nil
}

func (c *RewardContract) GetBalance(ctx contractapi.TransactionContextInterface, id string) (*Balance, error) {
	b, err := ctx.GetStub().GetState(balanceKey(id))
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, fmt.Errorf("balance missing")
	}
	var balance Balance
	if err = json.Unmarshal(b, &balance); err != nil {
		return nil, err
	}
	return &balance, nil
}

func (c *RewardContract) SetBalance(ctx contractapi.TransactionContextInterface, id string, amount int) error {
	b, err := json.Marshal(Balance{UserID: id, Amount: amount})
	if err != nil {
		return err
	}
	return ctx.GetStub().PutState(balanceKey(id), b)
}

func (c *RewardContract) InitLedger(ctx contractapi.TransactionContextInterface) error { return nil }

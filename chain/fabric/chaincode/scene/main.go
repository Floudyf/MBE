package main
import "github.com/hyperledger/fabric-contract-api-go/contractapi"
func main(){c,_:=contractapi.NewChaincode(new(SceneContract));_ = c.Start()}

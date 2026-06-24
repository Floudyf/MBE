package main
import "github.com/hyperledger/fabric-contract-api-go/contractapi"
func main(){c,_:=contractapi.NewChaincode(new(RewardContract));_ = c.Start()}

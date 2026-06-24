package main
import "github.com/hyperledger/fabric-contract-api-go/contractapi"
func main(){ c,err:=contractapi.NewChaincode(new(AssetContract)); if err!=nil {panic(err)}; if err:=c.Start();err!=nil {panic(err)} }

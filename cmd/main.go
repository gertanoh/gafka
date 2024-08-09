package main

import "go.uber.org/zap"

func InitLogger() {
	logg, _ := zap.NewDevelopment(zap.AddCaller())
	zap.ReplaceGlobals(logg)
}

func init() {
	InitLogger()
}

func main() {
	zap.S().Info("Starting App")
}

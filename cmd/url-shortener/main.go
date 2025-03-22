package main

import (
	"fmt"
	"url-shortener/internal/config"
)

func main() {
	cfg := config.MustLoad()

	fmt.Println(cfg)
	//TODO: init config: cleanenv

	//TODO: init logger: slog

	//TODO: init storage: sqlite

	//TODO: init router: chi, "chi render"

	//TODO: init server
}

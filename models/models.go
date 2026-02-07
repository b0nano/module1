package models

import "time"

type Item struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type Order struct {
	ID       string    `json:"id"`
	ClientID string    `json:"client_id"`
	Created  time.Time `json:"created"`
	Items    []Item    `json:"items"`
	Total    float64   `json:"total"`
}

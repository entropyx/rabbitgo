package utils

import (
  "time"
  "math/rand"
)

func RandomString(l int) string {
  rand.Seed(time.Now().UnixNano())
  bytes := make([]byte, l)
  for i := 0; i < l; i++ {
    bytes[i] = byte(randInt(65, 90))
  }
  return string(bytes)
}

func randInt(min int, max int) int {
  return min + rand.Intn(max-min)
}

package main

import (
	"context"
	"fmt"
	"github.com/avi-yadav/go-base-repo/base-repo"
	"sync"
)

type User struct {
	base_repo.BaseModel
	Name string
	Age  int
}

// Implement Entity interface
func (u *User) GetBase() *base_repo.BaseModel {
	return &u.BaseModel
}

func main() {
	ctx := context.Background()
	userRepo := base_repo.NewRepo[*User]()

	// Create
	u1 := &User{Name: "Alice", Age: 30}
	id := userRepo.Create(ctx, u1)
	fmt.Println("Created ID:", id)

	// Get
	u, _ := userRepo.Get(ctx, id)
	fmt.Println("Got:", u.Name, u.Age)

	// Update
	u.Age = 31
	userRepo.Update(ctx, u)

	// FindFirst
	u2, ok := userRepo.FindFirst(ctx, func(u *User) bool { return u.Age > 30 })
	if ok {
		fmt.Println("First user with age >30:", u2.Name)
	}

	// Count
	fmt.Println("Total users:", userRepo.Count(ctx, nil))

	// Direct store access
	userRepo.WithStore(func(store *sync.Map) {
		store.Range(func(_, value any) bool {
			user := value.(*User)
			user.Age++
			return true
		})
	})

	// Verify bulk update
	u, _ = userRepo.Get(ctx, id)
	fmt.Println("Age after bulk update:", u.Age)

	// Delete
	userRepo.Delete(ctx, id)
	fmt.Println("Exists after delete:", userRepo.Exists(ctx, id))
}

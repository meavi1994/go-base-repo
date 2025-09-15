package base_repo

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"testing"
)

type User struct {
	BaseModel
	Name  string
	Email string
}

func (u *User) GetBase() *BaseModel {
	return &u.BaseModel
}

func (u *User) String() string {
	return fmt.Sprintf("(%v, %v)", u.Name, u.Email)
}
func UserName(u *User) string {
	return u.Name
}

func UserEmail(u *User) string {
	return u.Email
}

func TestNewRepo(t *testing.T) {
	ctx := context.Background()
	userRepo := NewRepo[*User]()
	userRepo.AddIndex(ctx, "by_name", NewIndex[string, *User](UserName, cmp.Less[string]))
	userRepo.AddIndex(ctx, "by_email", NewIndex[string, *User](UserEmail, cmp.Less[string]))
	userRepo.Create(ctx, &User{
		Name:  "john",
		Email: "john@abc.com",
	})
	userRepo.Create(ctx, &User{
		Name:  "doe",
		Email: "test@abc.com",
	})
	userRepo.Create(ctx, &User{
		Name:  "doe",
		Email: "on@abc.com",
	})
	print(ctx, userRepo)

}
func logger(event Event[*User]) {
	fmt.Println("received : ", event.Type, "->", event.Obj)
}

func consumer(ch chan Event[*User]) {
	for e := range ch {
		logger(e)
	}
}

func TestEvents(t *testing.T) {
	ctx := context.Background()
	userRepo := NewRepo[*User]()
	ch := make(chan Event[*User])
	userRepo.AddSubscriber(ch)
	go consumer(ch)
	userRepo.AddIndex(ctx, "by_name", NewIndex[string, *User](UserName, cmp.Less[string]))
	userRepo.AddIndex(ctx, "by_email", NewIndex[string, *User](UserEmail, cmp.Less[string]))
	userRepo.Create(ctx, &User{
		Name:  "john",
		Email: "john@abc.com",
	})
	userRepo.Create(ctx, &User{
		Name:  "doe",
		Email: "test@abc.com",
	})
	userRepo.Create(ctx, &User{
		Name:  "doe",
		Email: "on@abc.com",
	})

	print(ctx, userRepo)

}

func TestUniqueNewRepo(t *testing.T) {
	ctx := context.Background()
	userRepo := NewRepo[*User]()
	userRepo.AddIndex(ctx, "by_name", NewUniqueIndex[string, *User](UserName, cmp.Less[string]))
	userRepo.AddIndex(ctx, "by_email", NewIndex[string, *User](UserEmail, cmp.Less[string]))
	_, err := userRepo.Create(ctx, &User{
		Name:  "john",
		Email: "john@abc.com",
	})
	if err != nil {
		log.Fatalf("failed1: %v", err)
	}
	_, err = userRepo.Create(ctx, &User{
		Name:  "doe",
		Email: "test@abc.com",
	})
	if err != nil {
		log.Fatalf("failed2: %v", err)
	}
	_, err = userRepo.Create(ctx, &User{
		Name:  "doe",
		Email: "on@abc.com",
	})
	if err == nil {
		log.Fatalf("failed3: %v", err)
	}
	fmt.Println(err)
}

func print(ctx context.Context, userRepo Repo[*User]) {
	byName, _ := userRepo.GetIndex(ctx, "by_name")
	byEmail, _ := userRepo.GetIndex(ctx, "by_email")
	fmt.Println("repo ->", userRepo)
	fmt.Println("name ->", byName)
	fmt.Println("email ->", byEmail)

}

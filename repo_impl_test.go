package base_repo

import (
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
	userRepo.AddIndex("by_name", NewIndex[*User](UserName))
	userRepo.AddIndex("by_email", NewIndex[*User](UserEmail))
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
	print(userRepo)

}
func logger(event string, u *User) {
	fmt.Println("received : ", event, "->", u)
}

func TestEvents(t *testing.T) {
	ctx := context.Background()
	userRepo := NewRepo[*User]()
	userRepo.AddSubscriber(logger)
	userRepo.AddIndex("by_name", NewIndex[*User](UserName))
	userRepo.AddIndex("by_email", NewIndex[*User](UserEmail))
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

	print(userRepo)
	userRepo.WaitTillNotificationDone()

}

func TestUniqueNewRepo(t *testing.T) {
	ctx := context.Background()
	userRepo := NewRepo[*User]()
	userRepo.AddIndex("by_name", NewUniqueIndex[*User](UserName))
	userRepo.AddIndex("by_email", NewIndex[*User](UserEmail))
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

func print(userRepo Repo[*User]) {
	byName, _ := userRepo.GetIndex("by_name")
	byEmail, _ := userRepo.GetIndex("by_email")
	fmt.Println("repo ->", userRepo)
	fmt.Println("name ->", byName)
	fmt.Println("email ->", byEmail)

}

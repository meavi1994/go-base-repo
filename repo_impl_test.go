package base_repo

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
)

type User struct {
	BaseModel
	Name  string
	Email string
}

func (u *User) String() string {
	return fmt.Sprintf("(%v, %v)", u.Name, u.Email)
}
func UserNameLessFunc(a, b *User) bool {
	return a.Name < b.Name
}

func UserEmailLessFunc(u, b *User) bool {
	return u.Email < b.Email
}

func TestNewRepo(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	indexes := map[string]BTreeG[*User]{
		"user_name":  NewSafeBTreeG[*User](2, UserNameLessFunc),
		"user_email": NewSafeBTreeG[*User](2, UserEmailLessFunc),
	}
	userRepo := NewBaseRepo[*User](indexes, nil)

	userRepo.Add(&User{
		Name:  "john",
		Email: "john@abc.com",
	})
	userRepo.Add(&User{
		Name:  "doe",
		Email: "test@abc.com",
	})
	userRepo.Add(&User{
		Name:  "doe",
		Email: "on@abc.com",
	})
	fmt.Println(userRepo)
	fmt.Println("first index")
	userNameIndex, _ := userRepo.GetIndex("user_name")
	for val := range userNameIndex.Ascend {
		fmt.Println(val)
	}
	fmt.Println("second index")
	userEmailIndex, _ := userRepo.GetIndex("user_email")
	for val := range userEmailIndex.Ascend {
		fmt.Println(val)
	}

}
func logger(event Event[*User]) {
	fmt.Println("received : ", event.Type, "->", event.Val)
}

func consumer(ch chan Event[*User]) {
	for e := range ch {
		logger(e)
	}
}

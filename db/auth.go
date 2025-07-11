package db

import (
	"fmt"
	"github.com/pocketbase/pocketbase/core"

	"github.com/pocketbase/pocketbase/tools/types"
	"strings"
)

const UserCollName = "users"
const UserFieldUsername = "username"
const UserFieldName = "name"
const UserFieldEmail = "email"
const UserFieldAvatar = "avatar"
const UserFieldAdmin = "admin"

const FieldAdmin = "admin"

func NewAuth(collectionName string, fieldKey string, user *User, roles []string, env Env) Auth {
	collectionAuthName := collectionName + "_auth"
	return Auth{
		CollectionBase: CollectionBase{Name: collectionName, Env: env},
		CollectionAuth: CollectionBase{Name: collectionAuthName, Env: env},
		AuthBuilder:    NewAuthorizationBuilder(collectionAuthName, fieldKey, roles),
		User:           user,
	}
}

type Auth struct {
	CollectionBase
	CollectionAuth CollectionBase
	AuthBuilder    *AuthorizationBuilder
	User           *User
}

func (auth *Auth) Load() (err error) {
	if auth.CollectionAuth.Collection != nil && !auth.IsRecreateDb() {
		return
	}

	dao := auth.App()
	if auth.CollectionAuth.Collection, err = dao.FindCollectionByNameOrId(auth.AuthBuilder.CollectionName); auth.CollectionAuth.Collection == nil ||
		(auth.IsRecreateDb() && auth.IsRecreateDbAuth()) {

		if auth.CollectionAuth.Collection != nil {
			if err = dao.Delete(auth.CollectionAuth.Collection); err != nil {
				return
			}
		}

		auth.CollectionAuth.Collection = core.NewBaseCollection(auth.AuthBuilder.CollectionName)
		auth.CollectionAuth.Collection.Fields.Add(
			&core.TextField{
				Name:     auth.AuthBuilder.FieldKey,
				Min:      2,
				Pattern:  "",
				Required: true,
			},
		)
		auth.CollectionAuth.Collection.AddIndex(fmt.Sprintf("idx_%v_%v",
			auth.AuthBuilder.CollectionName, auth.AuthBuilder.FieldKey), true, auth.AuthBuilder.FieldKey, "")

		for _, role := range auth.AuthBuilder.Roles {

			auth.CollectionAuth.Collection.Fields.Add(
				&core.RelationField{
					Name:          auth.AuthBuilder.AuthFieldFor(role),
					CollectionId:  auth.User.Id,
					CascadeDelete: false,
				},
			)
		}

		err = dao.Save(auth.CollectionAuth.Collection)
	}
	return
}

func NewUser(env Env) *User {
	return &User{
		CollectionBase: CollectionBase{Name: UserCollName, Env: env},
	}
}

type User struct {
	CollectionBase
}

func (user *User) Load() (err error) {

	dao := user.App()
	if user.Collection == nil {
		if user.Collection, err = dao.FindCollectionByNameOrId(UserCollName); err != nil {
			return
		}
	}
	if _, ok := user.Collection.Fields.AsMap()[FieldAdmin]; !ok {
		user.Collection.Fields.Add(
			&core.BoolField{
				Name: FieldAdmin,
			},
		)
		err = dao.Save(user.Collection)
	}
	return
}

type UserInfo struct {
	Email    string `json:"email,omitempty"`
	Name     string `json:"name,omitempty"`
	Username string `json:"username,omitempty"`
	Admin    bool   `json:"admin,omitempty"`
}

func NewAuthorizationBuilder(collectionName string, fieldKey string, roles []string) (ret *AuthorizationBuilder) {
	ret = &AuthorizationBuilder{
		CollectionName: collectionName,
		FieldKey:       fieldKey,

		Roles: roles,

		CollectionUsers:           UserCollName,
		CollectionUsersFieldAdmin: FieldAdmin,
	}
	ret.Init()
	return
}

type AuthorizationBuilder struct {
	CollectionName string
	FieldKey       string

	CollectionUsers           string
	CollectionUsersFieldAdmin string

	Roles []string

	AuthGlobalAdmin string
	AuthLoggedIn    string

	rolesCount          int
	authCollectionKeyIn string
}

func (o *AuthorizationBuilder) Init() {
	o.rolesCount = len(o.Roles)
	o.AuthGlobalAdmin = AuthGlobalAdmin(o.CollectionUsersFieldAdmin)
	o.AuthLoggedIn = "@request.auth.id != \"\""
	o.authCollectionKeyIn = fmt.Sprintf("%v ?= @collection.%v.%v", o.FieldKey, o.CollectionName, o.FieldKey)
}

func (o *AuthorizationBuilder) ListRule() string {
	return o.ruleForRoles(o.Roles...)
}

func (o *AuthorizationBuilder) ViewRule() string {
	return o.ruleForRoles(o.Roles...)
}

func (o *AuthorizationBuilder) CreateRule() string {
	return o.AuthGlobalAdmin
}

func (o *AuthorizationBuilder) UpdateRule() (ret string) {
	if o.rolesCount >= 2 {
		ret = o.ruleForRoles(o.Roles[0], o.Roles[1])
	} else if o.rolesCount == 1 {
		ret = o.ruleForRoles(o.Roles[0])
	} else {
		ret = o.ruleForRoles()
	}
	return
}

func (o *AuthorizationBuilder) DeleteRule() string {
	return o.ruleForRoles(o.Roles[0])
}

func (o *AuthorizationBuilder) AuthFieldFor(role string) string {
	return fmt.Sprintf("%vs", role)
}

func (o *AuthorizationBuilder) authRoleFor(role string) string {
	return fmt.Sprintf("@request.auth.id ?= @collection.%v.%v.id", o.CollectionName, o.AuthFieldFor(role))
}

func (o *AuthorizationBuilder) ruleForRoles(roles ...string) (ret string) {
	authRoles := strings.Builder{}
	if o.rolesCount > 0 {
		for i, role := range roles {
			if i > 0 {
				authRoles.WriteString(" || ")
			}
			authRoles.WriteString(o.authRoleFor(role))
		}
		ret = fmt.Sprintf("%v || ( %v && %v && ( %v ))", o.AuthGlobalAdmin, o.AuthLoggedIn,
			o.authCollectionKeyIn, authRoles.String())
	} else {
		ret = o.AuthGlobalAdmin
	}
	return
}

func AuthGlobalAdmin(fieldAdmin string) string {
	return fmt.Sprintf("@request.auth.%v = true", fieldAdmin)
}

func DisableAuth(coll *core.Collection) {
	coll.ListRule = types.Pointer("")
	coll.ViewRule = types.Pointer("")
	coll.CreateRule = types.Pointer("")
	coll.UpdateRule = types.Pointer("")
	coll.DeleteRule = types.Pointer("")
}

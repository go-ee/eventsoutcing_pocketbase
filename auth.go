package espocketbase

import (
	"fmt"
	"strings"
)

const FieldAdmin = "admin"

func NewAuthorizationBuilder(collectionName string, fieldKey string, roles []string) (ret *AuthorizationBuilder) {
	ret = &AuthorizationBuilder{
		CollectionName: collectionName,
		FieldKey:       fieldKey,

		Roles: roles,

		CollectionUsers:           UsersCollName,
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

	rolesCount int

	AuthGlobalAdmin     string
	AuthLoggedIn        string
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
	return fmt.Sprintf("auth_%vs", role)
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

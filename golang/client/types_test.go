package client_test

import (
	"fmt"
	"testing"

	"github.com/mister-webhooks/mister-webhooks-client/client"
	"github.com/stretchr/testify/assert"
)

var httpMethods = []client.HTTPMethod{
	client.HTTPMethodGET,
	client.HTTPMethodHEAD,
	client.HTTPMethodPOST,
	client.HTTPMethodPUT,
	client.HTTPMethodDELETE,
	client.HTTPMethodPATCH,
}

func TestHTTPMethodEncoding(t *testing.T) {
	for _, method := range httpMethods {
		t.Run(fmt.Sprintf("HTTP Method %s rountrips", method), func(t *testing.T) {
			text, err := method.MarshalText()

			assert.NoError(t, err)

			decoded := new(client.HTTPMethod)

			err = decoded.UnmarshalText(text)

			assert.NoError(t, err)
			assert.Equal(t, method, *decoded)
		})

	}
}

func TestHTTPMethodInvalid(t *testing.T) {
	invalid := *new(client.HTTPMethod)

	assert.Equal(t, "<http_method_invalid>", invalid.String())

	text, err := invalid.MarshalText()

	assert.Nil(t, text)
	assert.Error(t, err)
}

func TestHTTPMethodUnknown(t *testing.T) {
	unknown := new(client.HTTPMethod)

	err := unknown.UnmarshalText([]byte("this is bad input"))

	assert.Error(t, err)

	*unknown = 0xef

	assert.Equal(t, "<http_method_unknown(0xef)>", unknown.String())
}

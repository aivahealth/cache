package cache

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	goredislib "github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// TODO: rewrite using DEntry (see distrib_cache.go)

type Oauth2Client struct {
	redisClient          *goredislib.Client
	redisMutex           *redsync.Mutex
	spec                 *Spec
	lastKnownCredentials *Credentials
	useCaseKey           string
}

type GrantType string

const (
	GrantTypeUnspecified               GrantType = "GrantTypeUnspecified"
	GrantTypeClientCredentialsExchange           = "GrantTypeClientCredentialsExchange"
)

type Spec struct {
	GrantType      GrantType
	ClientId       string
	ClientSecret   string
	TokenUrl       string
	AuthUrl        string
	Scopes         []string
	EndpointParams url.Values
}

type Credentials struct {
	AccessToken  string
	RefreshToken string
	Expiry       time.Time
}

// i.e. not-expired
func (creds *Credentials) Valid(lookahead time.Duration) bool {
	return creds != nil && time.Since(creds.Expiry)+lookahead <= 0
}

// Keys for the Redis hash for storing a Credentials object
const (
	redisKeyCredentialsAccessToken  = "access_token"
	redisKeyCredentialsRefreshToken = "refresh_token"
	redisKeyCredentialsExpiry       = "expiry"
)

func NewOauth2Client(
	redisClient *goredislib.Client,
	// By default, we lock and store data for this client by oauth2 clientid
	// If that's insufficient, use this string, which will be applied as a suffix
	useCaseKey string,
	spec *Spec,
) *Oauth2Client {
	newClient := &Oauth2Client{
		redisClient: redisClient,
		spec:        spec,
		useCaseKey:  useCaseKey,
	}
	newClient.redisMutex = DLock(newClient.uniqueKey())
	logger().Debug(fmt.Sprintf("cache.Oauth2Client initialized for %q", newClient.uniqueKey()))
	return newClient
}

func (c *Oauth2Client) uniqueKey() string {
	if c.useCaseKey == "" {
		return c.spec.ClientId
	}
	return fmt.Sprintf("%s:%s", c.spec.ClientId, c.useCaseKey)
}

func (c *Oauth2Client) credentialsKeyName() string {
	return fmt.Sprintf("distributed_oauth2:credentials:%s", c.uniqueKey())
}

func (c *Oauth2Client) LatestCredentials(ctx context.Context) (*Credentials, error) {
	return c.LatestCredentialsWithLookahead(ctx, 0)
}

// Synchronously gets latest credentials, refreshing them if necessary
// If lookahead is non-zero, we'll advance the clock when checking token freshness
func (c *Oauth2Client) LatestCredentialsWithLookahead(ctx context.Context, lookahead time.Duration) (*Credentials, error) {
	// We can skip all interactions with redis in most cases...
	if c.lastKnownCredentials != nil && c.lastKnownCredentials.Valid(lookahead) {
		return c.lastKnownCredentials, nil
	}

	// Distributed lock!
	if err := c.redisMutex.LockContext(ctx); err != nil {
		return nil, err
	}
	defer func() {
		c.redisMutex.UnlockContext(ctx)
		logger().Debug("[distributed_oauth2] Lock released", zap.String("key", c.uniqueKey()))
	}()
	logger().Debug("[distributed_oauth2] Lock acquired", zap.String("key", c.uniqueKey()))

	// Load up the latest credentials from storage
	latestCreds, err := c.loadCredentials(ctx)
	if err != nil {
		return nil, err
	}

	if latestCreds == nil {
		// An initial fetch is required
		switch c.spec.GrantType {
		case GrantTypeClientCredentialsExchange:
			latestCreds, err = c.oauth2GetNewTokenCCE(ctx)
			if err != nil {
				return nil, err
			}
		default:
			err := fmt.Errorf("Unsupported GrantType: %s", c.spec.GrantType)
			logger().Error("[distributed_oauth2] Initial CCE fetch failed", zap.Error(err), zap.String("unique_key", c.uniqueKey()))
			return nil, err
		}
	} else if !latestCreds.Valid(lookahead) {
		// A refresh is required
		switch c.spec.GrantType {
		case GrantTypeClientCredentialsExchange:
			latestCreds, err = c.oauth2RefreshCCE(ctx, latestCreds)
			if err != nil {
				return nil, err
			}
		default:
			err := fmt.Errorf("Unsupported GrantType: %s", c.spec.GrantType)
			logger().Error("[distributed_oauth2] CCE refresh failed", zap.Error(err), zap.String("unique_key", c.uniqueKey()))
			return nil, err
		}
	}

	// Persist the latest creds
	err = c.persistCredentials(ctx, latestCreds)
	if err != nil {
		return nil, err
	}

	c.lastKnownCredentials = latestCreds

	logger().Debug(fmt.Sprintf("[distributed_oauth2] Credentials returned (expiring in %v)", time.Until(latestCreds.Expiry)), zap.String("key", c.uniqueKey()))

	// Done!

	return latestCreds, nil
}

func (c *Oauth2Client) persistCredentials(ctx context.Context, credsIn *Credentials) error {
	expiryUnix := credsIn.Expiry.Unix()
	expiryUnixString := strconv.FormatInt(expiryUnix, 10)

	key := c.credentialsKeyName()
	_, err := c.redisClient.HSet(
		ctx,
		key,
		redisKeyCredentialsAccessToken, credsIn.AccessToken,
		redisKeyCredentialsRefreshToken, credsIn.RefreshToken,
		redisKeyCredentialsExpiry, expiryUnixString,
	).Result()
	if err != nil {
		logger().Error("[distributed_oauth2] persistCredentials failed", zap.Error(err), zap.String("key", c.credentialsKeyName()))
		return err
	}
	return nil
}

func (c *Oauth2Client) loadCredentials(ctx context.Context) (*Credentials, error) {
	key := c.credentialsKeyName()
	exists, err := c.redisClient.Exists(ctx, key).Result()
	if err != nil {
		logger().Error("[distributed_oauth2] loadCredentials failed", zap.Error(err), zap.String("key", key))
		return nil, err
	}
	if exists == 0 {
		logger().Debug("[distributed_oauth2] loadCredentials: key does not exist in redis", zap.String("key", key))
		return nil, nil
	}

	resp, err := c.redisClient.HGetAll(ctx, c.credentialsKeyName()).Result()
	if err != nil {
		logger().Error("[distributed_oauth2] loadCredentials failed", zap.Error(err), zap.String("key", key))
		return nil, err
	}
	expiryUnix, err := strconv.ParseInt(resp[redisKeyCredentialsExpiry], 10, 64)
	if err != nil {
		logger().Error("[distributed_oauth2] loadCredentials failed", zap.Error(err), zap.String("key", key))
		return nil, err
	}
	expiry := time.Unix(expiryUnix, 0)

	return &Credentials{
		AccessToken:  resp[redisKeyCredentialsAccessToken],
		RefreshToken: resp[redisKeyCredentialsRefreshToken],
		Expiry:       expiry,
	}, nil
}

///////////////////////////////////////////////////////////////////////////////
// OAuth2 Operations
///////////////////////////////////////////////////////////////////////////////

// We (ab)use the golang.org/x/oauth2 library for these operations.

func (c *Oauth2Client) oauth2RefreshCCE(ctx context.Context, credsIn *Credentials) (*Credentials, error) {
	logger().Debug("[distributed_oauth2] oauth2RefreshCCE", zap.String("client_id", c.spec.ClientId), zap.Duration("since_expiry", time.Since(credsIn.Expiry)))
	cfg := clientcredentials.Config{
		ClientID:       c.spec.ClientId,
		ClientSecret:   c.spec.ClientSecret,
		TokenURL:       c.spec.TokenUrl,
		Scopes:         c.spec.Scopes,
		EndpointParams: c.spec.EndpointParams,
	}
	reusedToken := &oauth2.Token{
		AccessToken:  credsIn.AccessToken,
		RefreshToken: credsIn.RefreshToken,
		TokenType:    "Bearer",
		Expiry:       credsIn.Expiry,
	}
	// This SHOULD cause a refresh, assuming the passed-in expiry is earlier than now
	refreshed, err := oauth2.ReuseTokenSource(reusedToken, cfg.TokenSource(ctx)).Token()
	if err != nil {
		logger().Error("[distributed_oauth2] oauth2Refresh failed", zap.Error(err))
		return nil, err
	}
	return &Credentials{
		AccessToken:  refreshed.AccessToken,
		RefreshToken: refreshed.RefreshToken,
		Expiry:       refreshed.Expiry,
	}, nil
}

func (c *Oauth2Client) oauth2GetNewTokenCCE(ctx context.Context) (*Credentials, error) {
	logger().Debug("[distributed_oauth2] oauth2GetNewTokenCCE", zap.String("client_id", c.spec.ClientId))
	cfg := clientcredentials.Config{
		ClientID:       c.spec.ClientId,
		ClientSecret:   c.spec.ClientSecret,
		TokenURL:       c.spec.TokenUrl,
		Scopes:         c.spec.Scopes,
		EndpointParams: c.spec.EndpointParams,
	}
	tok, err := cfg.TokenSource(ctx).Token()
	if err != nil {
		logger().Error("[distributed_oauth2] oauth2GetNewTokenCCE failed", zap.Error(err), zap.String("client_id", c.spec.ClientId))
		return nil, err
	}
	return &Credentials{
		AccessToken:  tok.AccessToken,
		RefreshToken: tok.RefreshToken,
		Expiry:       tok.Expiry,
	}, nil
}

// Copyright (c) 2016, 2023 Oracle and/or its affiliates. All rights reserved.

package iam

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/sdkutil"
)

// ConfigurationProvider wraps information about the account owner
type ConfigurationProvider interface {
	KeyProvider
	TenancyOCID() (string, error)
	UserOCID() (string, error)
	KeyFingerprint() (string, error)
	Region() (string, error)
	SecurityTokenFile() (string, error)
}

// IsConfigurationProviderValid Tests all parts of the configuration provider do not return an error
func IsConfigurationProviderValid(conf ConfigurationProvider) (ok bool, err error) {
	baseFn := []func() (string, error){conf.TenancyOCID, conf.UserOCID, conf.KeyFingerprint, conf.KeyID}
	for _, fn := range baseFn {
		_, err = fn()
		ok = err == nil
		if err != nil {
			return
		}
	}

	_, err = conf.PrivateRSAKey()
	ok = err == nil
	if err != nil {
		return
	}
	return true, nil
}

// rawConfigurationProvider allows a user to simply construct a configuration provider from raw values.
type rawConfigurationProvider struct {
	tenancy              string
	user                 string
	region               string
	fingerprint          string
	privateKey           string
	privateKeyPassphrase *string
}

// NewRawConfigurationProvider will create a ConfigurationProvider with the arguments of the function
func NewRawConfigurationProvider(tenancy, user, region, fingerprint, privateKey string, privateKeyPassphrase *string) ConfigurationProvider {
	return rawConfigurationProvider{tenancy, user, region, fingerprint, privateKey, privateKeyPassphrase}
}

func (p rawConfigurationProvider) PrivateRSAKey() (key *rsa.PrivateKey, err error) {
	return PrivateKeyFromBytes([]byte(p.privateKey), p.privateKeyPassphrase)
}

func (p rawConfigurationProvider) ExpirationTime() time.Time {
	// raw configs don't expire
	return time.Now().Add(24 * time.Hour)
}

func (p rawConfigurationProvider) KeyID() (keyID string, err error) {
	tenancy, err := p.TenancyOCID()
	if err != nil {
		return
	}

	user, err := p.UserOCID()
	if err != nil {
		return
	}

	fingerprint, err := p.KeyFingerprint()
	if err != nil {
		return
	}

	return fmt.Sprintf("%s/%s/%s", tenancy, user, fingerprint), nil
}

func (p rawConfigurationProvider) TenancyOCID() (string, error) {
	if p.tenancy == "" {
		return "", fmt.Errorf("tenancy OCID can not be empty")
	}
	return p.tenancy, nil
}

func (p rawConfigurationProvider) UserOCID() (string, error) {
	if p.user == "" {
		return "", fmt.Errorf("user OCID can not be empty")
	}
	return p.user, nil
}

func (p rawConfigurationProvider) SecurityTokenFile() (string, error) {
	return "", fmt.Errorf("RawConfigurationProvider does not support SecurityTokenFile")
}

func (p rawConfigurationProvider) KeyFingerprint() (string, error) {
	if p.fingerprint == "" {
		return "", fmt.Errorf("fingerprint can not be empty")
	}
	return p.fingerprint, nil
}

func (p rawConfigurationProvider) Region() (string, error) {
	return canStringBeRegion(p.region)
}

// fileConfigurationProvider. reads configuration information from a file
type fileConfigurationProvider struct {
	//The path to the configuration file
	ConfigPath string

	//The password for the private key
	PrivateKeyPassword string

	//The profile for the configuration
	Profile string

	//ConfigFileInfo
	FileInfo *configFileInfo
}

// ConfigurationProviderFromFile creates a configuration provider from a configuration file
// by reading the "DEFAULT" profile.
func ConfigurationProviderFromFile(configFilePath, privateKeyPassword string) (ConfigurationProvider, error) {
	return ConfigurationProviderFromFileWithProfile(configFilePath, "DEFAULT", privateKeyPassword)
}

// ConfigurationProviderFromFileWithProfile creates a configuration provider from a configuration file
// and the given profile.
func ConfigurationProviderFromFileWithProfile(configFilePath, profile, privateKeyPassword string) (ConfigurationProvider, error) {
	if configFilePath == "" {
		return nil, fmt.Errorf("config file path can not be empty")
	}

	expandedFilePath, ok := fileExists(configFilePath)
	if !ok {
		return nil, fmt.Errorf("config file %s does not exist", configFilePath)
	}

	return fileConfigurationProvider{
		ConfigPath:         expandedFilePath,
		PrivateKeyPassword: privateKeyPassword,
		Profile:            profile}, nil
}

type configFileInfo struct {
	UserOcid, Fingerprint, KeyFilePath, TenancyOcid, Region, Passphrase, SecurityTokenFile string
	PresentConfiguration                                                                   byte
}

const (
	hasTenancy = 1 << iota
	hasUser
	hasFingerprint
	hasRegion
	hasKeyFile
	hasPassphrase
	hasSecurityTokenFile
	none
)

var profileRegex = regexp.MustCompile(`^\[(.*)\]`)

func parseConfigFile(data []byte, profile string) (info *configFileInfo, err error) {

	if len(data) == 0 {
		return nil, fmt.Errorf("configuration file content is empty")
	}

	content := string(data)
	splitContent := strings.Split(content, "\n")

	//Look for profile
	for i, line := range splitContent {
		if match := profileRegex.FindStringSubmatch(line); match != nil && len(match) > 1 && match[1] == profile {
			start := i + 1
			return parseConfigAtLine(start, splitContent)
		}
	}

	return nil, fmt.Errorf("configuration file did not contain profile: %s", profile)
}

func parseConfigAtLine(start int, content []string) (info *configFileInfo, err error) {
	var configurationPresent byte
	info = &configFileInfo{}
	for i := start; i < len(content); i++ {
		line := content[i]
		if profileRegex.MatchString(line) {
			break
		}

		if !strings.Contains(line, "=") {
			continue
		}

		splits := strings.Split(line, "=")
		switch key, value := strings.TrimSpace(splits[0]), strings.TrimSpace(splits[1]); strings.ToLower(key) {
		case "passphrase", "pass_phrase":
			configurationPresent = configurationPresent | hasPassphrase
			info.Passphrase = value
		case "user":
			configurationPresent = configurationPresent | hasUser
			info.UserOcid = value
		case "fingerprint":
			configurationPresent = configurationPresent | hasFingerprint
			info.Fingerprint = value
		case "key_file":
			configurationPresent = configurationPresent | hasKeyFile
			info.KeyFilePath = value
		case "tenancy":
			configurationPresent = configurationPresent | hasTenancy
			info.TenancyOcid = value
		case "security_token_file":
			configurationPresent = configurationPresent | hasSecurityTokenFile
			info.SecurityTokenFile = value
		case "region":
			configurationPresent = configurationPresent | hasRegion
			info.Region = value
		}
	}
	info.PresentConfiguration = configurationPresent
	return

}

func openConfigFile(configFilePath string) (data []byte, err error) {
	expandedPath, err := sdkutil.ExpandPath(configFilePath)
	if err != nil {
		err = fmt.Errorf("can not read config file: %s due to: %s", configFilePath, err.Error())
		return
	}

	data, err = ioutil.ReadFile(expandedPath)
	if err != nil {
		err = fmt.Errorf("can not read config file: %s due to: %s", configFilePath, err.Error())
	}

	return
}

func readTokenFromFile(tokenFilePath string) (string, error) {
	expandedPath, err := sdkutil.ExpandPath(tokenFilePath)
	if err != nil {
		err = fmt.Errorf("can not read token file: %s due to: %s", tokenFilePath, err.Error())
		return "", err
	}

	data, err := ioutil.ReadFile(expandedPath)
	if err != nil {
		err = fmt.Errorf("can not read token file: %s due to: %s", tokenFilePath, err.Error())
		return "", err
	}

	token := strings.Replace(string(data), "\r\n", "", -1)
	return strings.Replace(token, "\n", "", -1), nil
}

func (p fileConfigurationProvider) String() string {
	return fmt.Sprintf("Configuration provided by file: %s", p.ConfigPath)
}

func (p fileConfigurationProvider) readAndParseConfigFile() (info *configFileInfo, err error) {
	if p.FileInfo != nil {
		return p.FileInfo, nil
	}

	if p.ConfigPath == "" {
		return nil, fmt.Errorf("configuration path can not be empty")
	}

	data, err := openConfigFile(p.ConfigPath)
	if err != nil {
		err = fmt.Errorf("error while parsing config file: %s. Due to: %s", p.ConfigPath, err.Error())
		return
	}

	p.FileInfo, err = parseConfigFile(data, p.Profile)
	return p.FileInfo, err
}

func presentOrError(value string, expectedConf, presentConf byte, confMissing string) (string, error) {
	if value == "" {
		return "", errors.New(confMissing + " configuration is missing from file")
	}

	if presentConf&expectedConf == expectedConf {
		return value, nil
	}
	return "", errors.New(confMissing + " configuration is missing from file")
}

func (p fileConfigurationProvider) TenancyOCID() (value string, err error) {
	info, err := p.readAndParseConfigFile()
	if err != nil {
		err = fmt.Errorf("can not read tenancy configuration due to: %s", err.Error())
		return
	}

	value, err = presentOrError(info.TenancyOcid, hasTenancy, info.PresentConfiguration, "tenancy")
	return
}

func (p fileConfigurationProvider) SecurityTokenFile() (value string, err error) {
	info, err := p.readAndParseConfigFile()
	if err != nil {
		err = fmt.Errorf("can not read security token configuration due to: %s", err.Error())
		return
	}

	value, err = presentOrError(info.SecurityTokenFile, hasSecurityTokenFile,
		info.PresentConfiguration, "security_token_file")
	return
}

func (p fileConfigurationProvider) UserOCID() (value string, err error) {
	info, err := p.readAndParseConfigFile()
	if err != nil {
		err = fmt.Errorf("can not read tenancy configuration due to: %s", err.Error())
		return
	}

	value, err = presentOrError(info.UserOcid, hasUser, info.PresentConfiguration, "user")
	return
}

func (p fileConfigurationProvider) KeyFingerprint() (value string, err error) {
	info, err := p.readAndParseConfigFile()
	if err != nil {
		err = fmt.Errorf("can not read tenancy configuration due to: %s", err.Error())
		return
	}
	value, err = presentOrError(info.Fingerprint, hasFingerprint, info.PresentConfiguration, "fingerprint")
	return
}

func (p fileConfigurationProvider) ExpirationTime() time.Time {
	// file configs don't expire
	return time.Now().Add(24 * time.Hour)
}

func (p fileConfigurationProvider) KeyID() (string, error) {
	info, err := p.readAndParseConfigFile()
	if err != nil {
		err = fmt.Errorf("can not read tenancy configuration due to: %s", err.Error())
		return "", err
	}

	// SecurityTokenFile providers return different format for KeyID
	sFile, err := p.SecurityTokenFile()
	if sFile != "" && err == nil {
		// open/read file, return "ST$" + value (minus newlines)
		token, err := readTokenFromFile(sFile)
		if err != nil {
			return "", err
		}
		return "ST$" + token, nil
	}

	return fmt.Sprintf("%s/%s/%s", info.TenancyOcid, info.UserOcid, info.Fingerprint), nil
}

func (p fileConfigurationProvider) PrivateRSAKey() (key *rsa.PrivateKey, err error) {
	info, err := p.readAndParseConfigFile()
	if err != nil {
		err = fmt.Errorf("can not read tenancy configuration due to: %s", err.Error())
		return
	}

	filePath, err := presentOrError(info.KeyFilePath, hasKeyFile, info.PresentConfiguration, "key file path")
	if err != nil {
		return
	}

	expandedPath, err := sdkutil.ExpandPath(filePath)
	if err != nil {
		err = fmt.Errorf("can not read PrivateKey %s from configuration file due to: %s", filePath, err.Error())
		return
	}

	pemFileContent, err := ioutil.ReadFile(expandedPath)
	if err != nil {
		err = fmt.Errorf("can not read PrivateKey %s from configuration file due to: %s", filePath, err.Error())
		return
	}

	password := p.PrivateKeyPassword

	if password == "" && ((info.PresentConfiguration & hasPassphrase) == hasPassphrase) {
		password = info.Passphrase
	}

	key, err = PrivateKeyFromBytes(pemFileContent, &password)
	return
}

func (p fileConfigurationProvider) Region() (value string, err error) {
	info, err := p.readAndParseConfigFile()
	if err != nil {
		err = fmt.Errorf("can not read region configuration due to: %s", err.Error())
		return
	}

	value, err = presentOrError(info.Region, hasRegion, info.PresentConfiguration, "region")
	if err != nil {
		// Attempt to read region from environment variable
		value = os.Getenv("OCI_REGION")
		if value == "" {
			return
		}
	}

	return canStringBeRegion(value)
}

var blankRegex = regexp.MustCompile("\\s")

func canStringBeRegion(stringRegion string) (region string, err error) {
	if blankRegex.MatchString(stringRegion) || stringRegion == "" {
		return "", fmt.Errorf("region can not be empty or have spaces")
	}
	return stringRegion, nil
}

func SessionTokenProviderFromFileWithProfile(configFilePath, profile, privateKeyPassword string) (ConfigurationProvider, error) {
	if profile == "" {
		profile = "DEFAULT"
	}
	provider, err := ConfigurationProviderFromFileWithProfile(configFilePath, profile, privateKeyPassword)
	if err != nil {
		return nil, err
	}
	// verify that the config specifies a security token file
	_, err = provider.SecurityTokenFile()
	if err != nil {
		return nil, err
	}
	// read the session token file, verify it has contents
	_, err = provider.KeyID()
	if err != nil {
		return nil, err
	}
	return provider, nil
}

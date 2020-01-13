// Copyright (c) 2016, 2020 Oracle and/or its affiliates. All rights reserved.

package iam

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"regexp"
	"strings"
)

// ConfigurationProvider wraps information about the account owner
type ConfigurationProvider interface {
	KeyProvider
	TenancyOCID() (string, error)
	UserOCID() (string, error)
	KeyFingerprint() (string, error)
	Region() (string, error)
}

// IsConfigurationProviderValid Tests all parts of the configuration provider do not return an error
func IsConfigurationProviderValid(conf ConfigurationProvider) (ok bool, err error) {
	baseFn := []func() (string, error){conf.TenancyOCID, conf.UserOCID, conf.KeyFingerprint, conf.Region, conf.KeyID}
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
// by reading the "DEFAULT" profile
func ConfigurationProviderFromFile(configFilePath, privateKeyPassword string) (ConfigurationProvider, error) {
	if configFilePath == "" {
		return nil, fmt.Errorf("config file path can not be empty")
	}

	return fileConfigurationProvider{
		ConfigPath:         configFilePath,
		PrivateKeyPassword: privateKeyPassword,
		Profile:            "DEFAULT"}, nil
}

// ConfigurationProviderFromFileWithProfile creates a configuration provider from a configuration file
// and the given profile
func ConfigurationProviderFromFileWithProfile(configFilePath, profile, privateKeyPassword string) (ConfigurationProvider, error) {
	if configFilePath == "" {
		return nil, fmt.Errorf("config file path can not be empty")
	}

	return fileConfigurationProvider{
		ConfigPath:         configFilePath,
		PrivateKeyPassword: privateKeyPassword,
		Profile:            profile}, nil
}

type configFileInfo struct {
	UserOcid, Fingerprint, KeyFilePath, TenancyOcid, Region, Passphrase string
	PresentConfiguration                                                byte
}

const (
	hasTenancy = 1 << iota
	hasUser
	hasFingerprint
	hasRegion
	hasKeyFile
	hasPassphrase
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
		case "region":
			configurationPresent = configurationPresent | hasRegion
			info.Region = value
		}
	}
	info.PresentConfiguration = configurationPresent
	return

}

func getHomeFolder() string {
	current, e := user.Current()
	if e != nil {
		//Give up and try to return something sensible
		home := os.Getenv("HOME")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return current.HomeDir
}

// cleans and expands the path if it contains a tilde , returns the expanded path or the input path as is if not expansion
// was performed
func expandPath(filepath string) (expandedPath string) {
	cleanedPath := path.Clean(filepath)
	expandedPath = cleanedPath
	if strings.HasPrefix(cleanedPath, "~") {
		rest := cleanedPath[2:]
		expandedPath = path.Join(getHomeFolder(), rest)
	}
	return
}

func openConfigFile(configFilePath string) (data []byte, err error) {
	expandedPath := expandPath(configFilePath)
	data, err = ioutil.ReadFile(expandedPath)
	if err != nil {
		err = fmt.Errorf("can not read config file: %s due to: %s", configFilePath, err.Error())
	}

	return
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

func (p fileConfigurationProvider) KeyID() (keyID string, err error) {
	info, err := p.readAndParseConfigFile()
	if err != nil {
		err = fmt.Errorf("can not read tenancy configuration due to: %s", err.Error())
		return
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

	expandedPath := expandPath(filePath)
	pemFileContent, err := ioutil.ReadFile(expandedPath)
	if err != nil {
		err = fmt.Errorf("can not read PrivateKey  from configuration file due to: %s", err.Error())
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
		return
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

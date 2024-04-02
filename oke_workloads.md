# Oracle NoSQL Database Go SDK: authenticating inside Oracle Cloud OKE Workload containers

The Oracle NoSQL go SDK supports running programs inside OKE Workload containers in OCI. To do
so, you must add the OCI go SDK to your dependencies, add a short bit of code to your app, and set up OCI policies to enable access to NoSQL.
These steps are detailed below.

You should read the OCI documentation at [Granting Workloads Access to OCI Resources](https://docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/contenggrantingworkloadaccesstoresources.htm#contengmanagingworkloads_topic-grantingworkloadaccesstoresources-golang). Also, this [tutorial](https://docs.oracle.com/en-us/iaas/developer-tutorials/tutorials/helidon-k8s/01oci-helidon-k8s-summary.htm) is helpful in understanding the steps to create and manage OCI OKE Workloads.

## Add OCI go SDK to your dependencies

To add the OC go SDK as a dependency to your project (if not already using it), run the following command in your top-level go module directory:

```
go get github.com/oracle/oci-go-sdk/v65@latest
```

## Add code to your app to use OKE Workload identity

The NoSQL go SDK has a slightly enhanced version of `ConfigurationProvider` interface than the OCI go SDK. As such, there is a bit of extra code needed to use the OKE workload configuration provider in a NoSQL in your app.

First, create a new struct and 2 methods that wrap the OCI `ConfigurationProviderWithClaimAccess`:

```
import ociauth "github.com/oracle/oci-go-sdk/v65/common/auth"


type myConfigProvider struct {
    ociauth.ConfigurationProviderWithClaimAccess
}

// ExpirationTime in this case always returns a time in the future, effectively setting no expiration.
func (c *myConfigProvider) ExpirationTime() time.Time {
    return time.Now().Add(time.Hour)
}

// SecurityTokenFile is not applicable to the OKE Workload case
func (c *myConfigProvider) SecurityTokenFile() (string, error) {
    return "", fmt.Errorf("myConfigurationProvider does not support SecurityTokenFile")
}
```

Then, in your code that creates a SignatureProvider to use with NoSQL, Add the following code to use OKE Workload identity:
```
    conf, err := ociauth.OkeWorkloadIdentityConfigurationProvider()
    if err != nil {
        return nil, fmt.Errorf("cannot create an Oke Provider: %v", err)
    }
    myconf := &myConfigProvider{conf}
    sp, err := iam.NewSignatureProviderWithConfiguration(myconf, compartmentID)
    if err != nil {
        return nil, fmt.Errorf("cannot create a Signature Provider: %v", err)
    }
```

## Create OCI Identity Policy to allow NoSQL access from your OKE Workload

This part follows the same pattern as described in  
 [Granting Workloads Access to OCI Resources](https://docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/contenggrantingworkloadaccesstoresources.htm#contengmanagingworkloads_topic-grantingworkloadaccesstoresources-golang). Here's an example policy to use as a guide:
```
Allow any-user to manage nosql-family in compartment app_compartment where all {
 request.principal.type = 'workload',
 request.principal.namespace = 'default',
 request.principal.service_account = 'app-testaccount',
 request.principal.cluster_id = 'ocid1.cluster.oc1.___________________________'i
}
```

Once complete, you should be able to build your app as usual, and deploy it to OKE Workload using `docker` and `kubectl` commands as described in the above-referenced [tutorial](https://docs.oracle.com/en-us/iaas/developer-tutorials/tutorials/helidon-k8s/01oci-helidon-k8s-summary.htm).

## Special note

Note: as of this writing, the OCI go SDK has a small incompatibility with OKE Workloads. See [this issue report](https://github.com/oracle/oci-go-sdk/issues/489) for more details. 
In short, you need to add two environment settings for the auth to work correctly. They can be in the Dockerfile, in `os.Setenv()` calls, or however else you wish to set the ennvironment for your app. Here's an example of the two values to set in a Dockerfile:
```
ENV OCI_RESOURCE_PRINCIPAL_VERSION 2.2
ENV OCI_RESOURCE_PRINCIPAL_REGION us-ashburn-1
```


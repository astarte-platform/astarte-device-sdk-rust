<!--
This file is part of Astarte.

Copyright 2023, 2026 SECO Mind Srl

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
-->

# Astarte object datastream example

This example will guide you through setting up a simple application running the Astarte SDK and
interacting with a local instance of Astarte.

The device generated with this example will perform the following:

- Use volatile storage for storing all the SDK instance data. Each instance of the SDK will be
  completely oblivious of previous instances.
- Transmit at intervals of 5 seconds a data aggregate to the local instance of Astarte.

If you have not already done so, make sure that you satisfy the
[common prerequisites](./../README.md#common-prerequisites) and have completed the
[common configuration](./../README.md#common-configuration) for the examples.

## Check transmitted data from the device

Upon connecting to the local Astarte instance, the instance SDK will start transmitting
periodically aggregate objects.

We can check if the publish from the device has been correctly received on the server using
`get-samples` as follows:

```
astartectl appengine
--appengine-url http://localhost:4002/ --realm-management-url http://localhost:4000/
--realm-key <REALM_NAME>_private.pem --realm-name <REALM_NAME>
devices get-samples <DEVICE_NAME>
org.astarte-platform.rust.examples.object-datastream.DeviceDatastream <COMM_ENDPOINT> -c 1
```

Where `<REALM>` is your realm's name, `<DEVICE_ID>` is the device ID from which the data has
been received and `<COMM_ENDPOINT>` is the common part of the aggregate endpoints.
For this example `<COMM_ENDPOINT>` is fixed to `/23`.

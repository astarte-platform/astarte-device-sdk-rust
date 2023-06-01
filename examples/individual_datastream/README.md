<!--
Copyright 2023 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Astarte individual datastream example

This example will guide you through setting up a simple application running the Astarte SDK and
interacting with a local instance of Astarte.

The device generated with this example will perform the following:
- Use volatile storage for storing all the SDK instance data. Each instance of the SDK will be
completely oblivious of previous instances.
- Transmit periodically two values to the local instance of Astarte.
- Change settings of a LED in a "virtual" array of LEDs when a "led_id/enable" message is received
from the local instance of Astarte.

If you have not already done so, make sure that you satisfy the
[common prerequisites](./../README.md#common-prerequisites) and have completed the
[common configuration](./../README.md#common-configuration) for the examples.

## Check transmitted data from the device

Now that our device is connected to Astarte and is sending periodically data we can check if
the Astarte cluster is storing correctly this data.

We can check if the publish from the device has been correctly received on the server using
`get-samples` as follows:
```
astartectl appengine --appengine-url http://localhost:4002/ --realm-key <REALM>_private.pem
    --realm-name <REALM> devices get-samples <DEVICE_ID>
    org.astarte-platform.rust.examples.individual-datastream.DeviceDatastream <ENDPOINT> -c 1
```
Where `<REALM>` is your realm's name, `<DEVICE_ID>` is the device ID from which the data has
been received and `<ENDPOINT>` is the endpoint we want to observe.

This will print the latest published data from the device that has been stored in the Astarte Cloud
instance.
To print a longer history of the published data change the number in `-c 1` to the history length
of your liking.

## Send data to the device from the astarte instance

With `send-data` we can publish new values on a server-owned interface.
The syntax is the following:
```
astartectl appengine
    --appengine-url http://localhost:4002/ --realm-management-url http://localhost:4000/
    --realm-key <REALM>_private.pem
    --realm-name <REALM> devices send-data <DEVICE_ID>
    org.astarte-platform.rust.examples.individual-datastream.ServerDatastream <ENDPOINT> <VALUE>
```
Where `<REALM>` is your realm name, `<DEVICE_ID>` is the device ID to send the data to,
`<ENDPOINT_TYPE>` is the Astarte type of the chosen endpoint, `<ENDPOINT>` is the endpoint
to send data to, which in this example shold be composed by a LED id and one of the two endpoints
(e.g. `/42/endpoint1`), and `<VALUE>` is the value to send.

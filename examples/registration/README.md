<!--
Copyright 2023 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Astarte Registration example

This simple example show how to use a pairing JWT to register a device to a local Astarte instance.
It will furthermore print the obtained credential secret to screen.

If you have not already done so, make sure that you satisfy the
[common prerequisites](./../README.md#common-prerequisites) and have completed the
[common configuration](./../README.md#common-configuration) for the examples.

## Device connection to Astarte

Once the example has been started it will automatically attempt to register the device to the remote
instance of Astarte.
The retrieved credential secret will be displayed in the terminal. Make sure to copy and store it
somewhere safe as there is no way to retrieve it later.

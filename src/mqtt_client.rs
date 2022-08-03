/*
 * This file is part of Astarte.
 *
 * Copyright 2021 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{pairing, AstarteError, AstarteOptions};
use log::debug;
use rumqttc::{AsyncClient, ConnectionError, Event, EventLoop};
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender};

#[derive(Clone)]
pub struct AstarteOutboundMessage {
    pub client_id: String,
    pub interface: String,
    pub path: String,
    pub buf: Vec<u8>,
    pub qos: rumqttc::QoS,
}

#[derive(Clone)]
pub struct IntrospectionMessage {
    pub client_id: String,
    pub introspection: String,
}

#[derive(Clone)]
pub struct EmptyCacheMessage {
    pub client_id: String,
}

#[derive(Clone)]
pub enum MqttOutboundType {
    Introspection(IntrospectionMessage),
    OutboundMessage(AstarteOutboundMessage),
    EmptyCache(EmptyCacheMessage),
    Subscription(String),
    Ready,
    CloseOutbound,
}

pub struct MQTTClient {
    pub outbound_tx_channel: Sender<MqttOutboundType>,
    pub inbound_rx_channel: MpscReceiver<Event>,
}

impl MQTTClient {
    pub async fn new(opts: &AstarteOptions) -> Result<MQTTClient, AstarteError> {
        let (outbound_tx, _) = tokio::sync::broadcast::channel(256);
        let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(32);

        MQTTClient::mqtt_init(opts, false, outbound_tx.clone(), inbound_tx).await?;
        Ok(MQTTClient {
            outbound_tx_channel: outbound_tx,
            inbound_rx_channel: inbound_rx,
        })
    }

    async fn mqtt_init(
        opts: &AstarteOptions,
        close_outbound: bool,
        outbound_tx: Sender<MqttOutboundType>,
        inbound_tx: MpscSender<Event>,
    ) -> Result<(), AstarteError> {
        let mqtt_options = pairing::get_transport_config(opts).await?;

        debug!("{:#?}", mqtt_options);

        // TODO: make cap configurable
        let (client, eventloop) = AsyncClient::new(mqtt_options.clone(), 50);

        if close_outbound {
            outbound_tx.send(MqttOutboundType::CloseOutbound);
        }

        MQTTClient::start_tasks(client, eventloop, outbound_tx, inbound_tx, opts);
        Ok(())
    }

    fn start_tasks(
        client: AsyncClient,
        eventloop: EventLoop,
        outbound_tx: Sender<MqttOutboundType>,
        inbound_tx: MpscSender<Event>,
        opts: &AstarteOptions,
    ) {
        let mut outbound_rx = outbound_tx.subscribe();
        let outbound_tx_clone = outbound_tx.clone();

        tokio::spawn(async move {
            let mut ready_to_send = false;
            while let Ok(msg) = outbound_rx.recv().await {
                let _ = match msg {
                    MqttOutboundType::OutboundMessage(data) => {
                        if !ready_to_send {
                            outbound_tx_clone.send(MqttOutboundType::OutboundMessage(data));
                        } else {
                            MQTTClient::send_data(&client, data).await?
                        }
                    }
                    MqttOutboundType::Introspection(introspection) => {
                        MQTTClient::send_introspection(&client, introspection).await?
                    }
                    MqttOutboundType::EmptyCache(empty_cache_message) => {
                        MQTTClient::send_emptycache(&client, empty_cache_message).await?
                    }
                    MqttOutboundType::Subscription(topic) => {
                        MQTTClient::subscribe(&client, topic).await?
                    }
                    MqttOutboundType::Ready => ready_to_send = true,
                    MqttOutboundType::CloseOutbound => break,
                };
            }
            Result::<(), AstarteError>::Ok(())
        });

        let astarte_options = opts.clone();

        tokio::spawn(async move {
            let output = MQTTClient::poll(eventloop, inbound_tx.clone()).await;
            if let Err(_) = output {
                // TODO: Instantiate a new Mqtt connection ONLY on certificate expired errors
                MQTTClient::mqtt_init(&astarte_options, true, outbound_tx.clone(), inbound_tx)
                    .await?;
            }
            Result::<(), AstarteError>::Ok(())
        });
    }

    async fn send_data(
        client: &AsyncClient,
        msg: AstarteOutboundMessage,
    ) -> Result<(), AstarteError> {
        client
            .publish(
                msg.client_id + "/" + msg.interface.trim_matches('/') + msg.path.as_str(),
                msg.qos,
                false,
                msg.buf,
            )
            .await?;
        Ok(())
    }

    async fn poll(
        mut eventloop: EventLoop,
        channel: MpscSender<Event>,
    ) -> Result<(), ConnectionError> {
        // -> rx Channel
        // loop in un thread a parte
        loop {
            // keep consuming and processing packets until we have data for the user
            let event = eventloop.poll().await?;
            let _ = channel.send(event).await;
        }
    }

    async fn send_emptycache(
        client: &AsyncClient,
        empty_cache_message: EmptyCacheMessage,
    ) -> Result<(), AstarteError> {
        let url = empty_cache_message.client_id + "/control/emptyCache";
        debug!("sending emptyCache to {}", url);

        client
            .publish(url, rumqttc::QoS::ExactlyOnce, false, "1")
            .await?;

        Ok(())
    }

    async fn send_introspection(
        client: &AsyncClient,
        introspection: IntrospectionMessage,
    ) -> Result<(), AstarteError> {
        debug!("sending introspection = {}", introspection.introspection);

        client
            .publish(
                introspection.client_id,
                rumqttc::QoS::ExactlyOnce,
                false,
                introspection.introspection.clone(),
            )
            .await?;
        Ok(())
    }

    async fn subscribe(client: &AsyncClient, topic: String) -> Result<(), AstarteError> {
        client.subscribe(topic, rumqttc::QoS::ExactlyOnce).await?;
        Ok(())
    }
}

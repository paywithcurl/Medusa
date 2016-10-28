defmodule Medusa.Adapter do
  @moduledoc """
  Behaviour for broker to communicate with Queue

  The objective of having this adapter is to be able to switch them if
  you have specific requirements, such as: using Redis, RabbitMQ, AQMP.
  """
  alias Medusa.Broker.Message

  @callback new_route(event :: String.t) :: :ok | :error
  @callback publish(event :: String.t, message :: Message) :: :ok | :error

end

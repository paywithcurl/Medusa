defmodule Medusa.Adapter do
  @moduledoc """
  Behaviour for broker to communicate with Queue

  The objective of having this adapter is to be able to switch them if
  you have specific requirements, such as: using Redis, RabbitMQ, AQMP.
  """

  @type event :: String.t
  @type fun :: (Medusa.Broker.Message.t -> any)
  @type message :: Medusa.Broker.Message.t
  @type opts :: Keyword.t

  @callback new_route(event, fun, opts) :: {:ok, any} | {:error, any}
  @callback publish(message) :: :ok | :error

end

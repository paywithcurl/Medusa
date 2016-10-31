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
  @type response :: :ok | {:ok, any} | :error | {:error, any}

  @callback new_route(event, fun, opts) :: response
  @callback publish(event, message) :: response

end

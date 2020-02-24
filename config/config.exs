# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

config :prestige, :session_opts,
  user: "report",
  catalog: "hive",
  schema: "default",
  url: "https://presto.prod.internal.smartcolumbusos.com"

config :logger, level: :warn

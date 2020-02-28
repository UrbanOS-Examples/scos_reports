NimbleCSV.define(CsvParser, [])

defmodule Reports.Easymile.Incidents do
  def run(params \\ []) do
    incidents = query(params)
    |> Stream.map(fn row -> for {key, val} <- row, into: %{}, do: {String.to_atom(key), val} end)
    |> Enum.into([])
    |> Enum.reduce({[], nil}, fn current, {incidents, last} ->
      if last && current.reason != last.reason && current.id == last.id do
        incident =
          current
          |> Map.put(:reason_pre, last.reason)
          |> Map.put(:speed_pre, last.speed)
          |> Map.put(:mode_pre, last.mode)
          |> Map.delete(:speed)

        {[incident | incidents], current}
      else
        {incidents, current}
      end
    end)
    |> elem(0)
    |> Enum.dedup()
    |> Enum.reject(fn i -> i.reason == "" end)

    report = incidents
    |> Enum.map(fn incident -> Map.values(incident) |> Enum.join(",") end)
    |> Enum.map(fn row -> row <> "\n" end)

    headers = incidents  |> Enum.take(1) |> Enum.map(fn incident -> Map.keys(incident) |> Enum.join(",") end) |> Enum.map(fn row -> row <> "\n" end)

    File.write!("#{__MODULE__}.csv", headers ++ report)
  end

  def download() do
    data = query()
    |> Stream.map(fn row -> for {key, val} <- row, into: %{}, do: {String.to_atom(key), val} end)
    |> Enum.into([])

    download = data
    |> Enum.map(fn incident -> Map.values(incident) |> Enum.join(",") end)
    |> Enum.map(fn row -> row <> "\n" end)

    headers = data |> Enum.take(1) |> Enum.map(fn incident -> Map.keys(incident) |> Enum.join(",") end) |> Enum.map(fn row -> row <> "\n" end)

    File.write!("#{__MODULE__}.download.csv", headers ++ download)
  end

  defp query(params \\ []) do
    Application.get_env(:prestige, :session_opts)
    |> Prestige.new_session()
    |> Prestige.stream!(statement(params))
    |> Stream.flat_map(&Prestige.Result.as_maps/1)
  end

  defp statement(params \\ []) do
    hours = Keyword.get(params, :hours, 24)
    "SELECT attributes.disengagement_reason as reason, attributes.last_seen, attributes.lat, attributes.lon, attributes.speed, attributes.mode, id FROM easymile__linden_states order by id desc, attributes.last_seen asc"
  end
end

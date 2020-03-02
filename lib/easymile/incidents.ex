NimbleCSV.define(CsvParser, [])

defmodule Reports.Easymile.Incidents do
  def run(params \\ []) do
    data = query(params)
    |> Stream.map(fn row -> for {key, val} <- row, into: %{}, do: {String.to_atom(key), val} end)
    |> Stream.map(fn row -> Map.update!(row, :last_seen, &date_parse/1) end)
    |> Enum.into([])

    IO.inspect("Got data")

    incidents =
      data
      |> Enum.reduce({[], [], nil}, fn current, {incidents, context, last} ->
        if last && current.reason != last.reason && current.id == last.id do
          incident =
            current
            |> Map.put(:reason_pre, last.reason)
            |> Map.put(:context, context |> Enum.take(10))
            |> Map.delete(:speed)

          {[incident | incidents], [], current}
        else
          {incidents, [current | context], current}
        end
      end)
      |> elem(0)
      |> Enum.dedup()
      |> Enum.reject(fn i -> i.reason == "" end)

    IO.inspect("Got incidents")

    incidents_with_context =
      incidents
      |> Enum.map(fn incident -> get_context(incident) end)

    IO.inspect("Got incident contexts")

    report =
      incidents_with_context
      |> Enum.map(fn incident -> Map.values(incident) |> Enum.join(",") end)
      |> Enum.map(fn row -> row <> "\n" end)

    headers =
      incidents_with_context
      |> Enum.take(1)
      |> Enum.map(fn incident -> Map.keys(incident) |> Enum.join(",") end)
      |> Enum.map(fn row -> row <> "\n" end)

    File.write!("#{__MODULE__}.csv", headers ++ report)
  end

  defp get_context(incident) do
    IO.puts("Getting context for incident at: #{incident.last_seen}")
    incident_time = incident.last_seen

    highest_speed_datum =
      Enum.filter(incident.context, fn datum ->
        time_since_incident = NaiveDateTime.diff(incident_time, datum.last_seen)
        (time_since_incident > 0 and time_since_incident <= 6)
      end)
      |> Enum.sort(fn a, b -> a.speed > b.speed end)
      |> List.first()

    if highest_speed_datum do
      %{
        disengagement_time: incident.last_seen,
        disengagement_mode: incident.mode,
        disengagement_reason: incident.reason,
        disengagement_prev_reason: incident.reason_pre,
        id: incident.id,
        lat: incident.lat,
        lon: incident.lon,
        initial_speed: highest_speed_datum.speed,
        initial_mode: highest_speed_datum.mode,
        initial_time: highest_speed_datum.last_seen
      }
    else
      %{
        disengagement_time: incident.last_seen,
        disengagement_mode: incident.mode,
        disengagement_reason: incident.reason,
        disengagement_prev_reason: incident.reason_pre,
        id: incident.id,
        lat: incident.lat,
        lon: incident.lon,
        initial_speed: nil,
        initial_mode: nil,
        initial_time: nil
      }
    end
  end

  defp date_parse(date), do: DateTime.from_iso8601(date <> "Z") |> elem(1)

  def download() do
    data =
      query()
      |> Stream.map(fn row -> for {key, val} <- row, into: %{}, do: {String.to_atom(key), val} end)
      |> Enum.into([])

    download =
      data
      |> Enum.map(fn incident -> Map.values(incident) |> Enum.join(",") end)
      |> Enum.map(fn row -> row <> "\n" end)

    headers =
      data
      |> Enum.take(1)
      |> Enum.map(fn incident -> Map.keys(incident) |> Enum.join(",") end)
      |> Enum.map(fn row -> row <> "\n" end)

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

  defp pmap(collection, func) do
    collection
    |> Enum.map(&(Task.async(fn -> func.(&1) end)))
    |> Enum.map(fn task -> Task.await(task, 15000) end)
  end
end

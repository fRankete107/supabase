import { createClient, SupabaseClient } from 'https://esm.sh/@supabase/supabase-js@2'
import { serve } from "https://deno.land/std@0.168.0/http/server.ts"

console.log("Hello from Functions!")

serve(async (req) => {
  const url = new URL(req.url);
  const query = {
    market : url.searchParams.get("market"),
    country : url.searchParams.get("country"),
    station : url.searchParams.get("station"),
    freq : url.searchParams.get("freq"),
    timestamp : url.searchParams.get("timestamp")
  };
  const jsonData = await req.json();
  const acrData = jsonData.metadata.music[0];
  console.log(acrData);
  const { title, artists, acrid }: { title: string, artists: Array<any>, acrid: string } = acrData;
  const { market, country, station, freq, timestamp }: { market: any, country: any, station: any, freq: any, timestamp: any } = query;
  //if (url.searchParams.market != undefined) {
  //  var { market, country, station, freq, timestamp }: { market: string, country: string, station: string, freq: string, timestamp: string } = url.searchParams.entries();
  //} else {
  //  var { market, country, station, freq, timestamp }: { market: string, country: string, station: string, freq: string, timestamp: string } = testQuery;
  //}

  const SUPABASE_URL: string = "https://gtjpquxczkowyjucrmdu.supabase.co";
  const SUPABASE_KEY: string = "***";

  let artist = "";
  for (let artist_ of artists) {
    if (typeof artist_.role == 'undefined') {
      artist = artist_.name;
      artist_.role = "";
    }
    if (artist_.role == "MainArtist") {
      (artist == "") ? artist = artist_.name : artist = artist + ", " + artist_.name;
    }
  }
  
  type Record = {
    id?: Number;
    market?: string;
    country?: string;
    station?: string;
    frequency?: string;
    title?: string;
    artist?: string;
    acr_id?: string;
    timestamp?: string;
  };

  const record: Record = {
    market: market,
    country: country,
    station: station,
    frequency: freq,
    title: title,
    artist: artist,
    acr_id: acrid,
    timestamp: timestamp
  }

  const supabase: SupabaseClient = createClient(SUPABASE_URL, SUPABASE_KEY);

  let res = {
    message: ""
  };

  const changeRes = (msg: string) => {
    res.message = msg
  };

  const { data, error } = await supabase
    .from("franco_data")
    .select("*")
    .eq("market", market)
    .eq("country", country)
    .eq("station", station)
    .eq("frequency", freq);
  
  const records: any = { data };

  const insertRecord = async (record: Record) => {
    const { error } = await supabase
      .from("franco_data")
      .insert(record);
    if (error) {
      console.log(error);
    } else {
    changeRes("Data added!")
    }
    

    if (error) {
      return new Response(
        JSON.stringify(error),
        { headers: { "Content-Type": "application/json" } },
      );
    }
  }

  const duplicate_records = async (record: Record) => {
    const { error } = await supabase
      .from("franco_data_dupe")
      .insert(record);
    if (res.message == "") {
      changeRes("Absolutly dupe!");
    }
    if (error) {
      return new Response(
        JSON.stringify(error),
        { headers: { "Content-Type": "application/json" } },
      );
    }
    return new Response(
      JSON.stringify(res),
      { headers: { "Content-Type": "application/json" } },
    );
  };

  const same_title = async (record: Record) => {
    changeRes("Same title.")
    await duplicate_records(record);
  };

  const same_artist = async (record:Record) => {
    changeRes("Same artist.")
    await duplicate_records(record);
  };

  if (error) {
    return new Response(
      JSON.stringify(error),
      { headers: { "Content-Type": "application/json" } },
    );
  }

  let duplicate = false;
  const last_viewed: any = records?.data ? records.data.slice(-10) : [];

  for (const viewed of last_viewed) {
    if (
      viewed.market == record.market &&
      viewed.country == record.country &&
      viewed.station == record.station &&
      viewed.frequency == record.frequency &&
      viewed.title == record.title &&
      viewed.artist == record.artist &&
      viewed.acr_id == record.acr_id
    ) {
      duplicate = true;
      await duplicate_records(record);
      break;
    }
    if (
      viewed.title == record.title &&
      viewed.market == record.market &&
      viewed.country == record.country &&
      viewed.station == record.station &&
      viewed.frequency == record.frequency
    ) {
      await same_title(record);
      const timestamp1 = viewed.timestamp;
      const timestamp2 = record.timestamp;
      if (!timestamp1) {
        changeRes("Not timestamp.")
        return new Response(
          JSON.stringify(res),
          { headers: { "Content-Type": "application/json" } },
        );
      }
      if (!timestamp2) {
        changeRes("Not timestamp.")
        return new Response(
          JSON.stringify(res),
          { headers: { "Content-Type": "application/json" } },
        );
      }
      const dt1 = new Date(timestamp1);
      const dt2 = new Date(timestamp2);
      const delta = Math.abs(dt2.getTime() - dt1.getTime()) / 1000;
      if (delta < 300) {
        duplicate = true;
        await duplicate_records(record)
        break;
      }
    }
    if (viewed.artist == record.artist) {
      await same_artist(record);
    }
  }
  if (!duplicate) {
    await insertRecord(record);
  }



  return new Response(
    JSON.stringify(res),
    { headers: { "Content-Type": "application/json" } },
  )
})

// To invoke:
//curl -i --location --request POST 'http://localhost:54321/functions/v1/' \
//  --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0' \
//  --header 'Content-Type: application/json' \
//  --data '{"name":"Functions"}'

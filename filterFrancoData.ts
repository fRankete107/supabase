import { createClient, SupabaseClient } from "@supabase/supabase-js";
require('dotenv').config();

export const filterFrancoData = async (): Promise<void> => {
  const SUPABASE_URL: string = process.env.SUPABASE_URL || 'default_value';
  const SUPABASE_KEY: string = process.env.SUPABASE_ANON_KEY || 'default_value';
  
  type Record = {
    market?: string;
    country?: string;
    station?: string;
    frequency?: string;
    title?: string;
    artist?: string;
    acr_id?: string;
    timestamp?: string;
  };
  const supabase: SupabaseClient = createClient(SUPABASE_URL, SUPABASE_KEY);

  const { data: data_list } = await supabase
    .from("device_raw_airplay")
    .select("*");

  const { error } = await supabase.rpc("truncate_franco_data", {});

  if (error) {
    console.log(error);
    return;
  }

  if (data_list == null) {
    return;
  }

  const records_viewed: Record[] = [];
  const same_title: any = [];
  const same_artist: any = [];

  const new_records: Record[] = [];

  const duplicate_records: Record[] = [];
  for (const record of data_list) {
    let duplicate = false;
    const last_viewed: Array<Record> = records_viewed.slice(-50);

    for (const viewed of last_viewed) {
      if (
        viewed.market === record.market &&
        viewed.country === record.country &&
        viewed.station === record.station &&
        viewed.frequency === record.frequency &&
        viewed.title === record.title &&
        viewed.artist === record.artist &&
        viewed.acr_id === record.acr_id
      ) {
        duplicate = true;
        duplicate_records.push(record);
        break;
      }
      if (
        viewed.title === record.title &&
        viewed.market === record.market &&
        viewed.country === record.country &&
        viewed.station === record.station &&
        viewed.frequency === record.frequency
      ) {
        const duo = [viewed, record];
        same_title.push(duo);
        const timestamp1 = viewed.timestamp;
        const timestamp2 = record.timestamp;
        if (!timestamp1) {
            return
        }
        const dt1 = new Date(timestamp1);
        const dt2 = new Date(timestamp2);
        const delta = Math.abs(dt2.getTime() - dt1.getTime()) / 1000;
        if (delta < 300) {
          duplicate = true;
          duplicate_records.push(record);
          break;
        }
      }
      if (viewed.artist === record.artist) {
        const duo: any[] = [viewed, record];
        same_artist.push(duo);
      }
    }
    if (!duplicate) {
      records_viewed.push(record);
      new_records.push(record);
    }
  }
  const { error: insertError } = await supabase
    .from("franco_data")
    .insert(new_records);

  if (insertError) {
    console.log(insertError);
    return;
  }
};


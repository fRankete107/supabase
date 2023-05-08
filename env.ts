import { writeTextFile } from "https://deno.land/std/fs/mod.ts";

const envContent = `SUPABASE_URL=https://tu-url-de-supabase
SUPABASE_KEY=tu-llave-de-supabase`;

await writeTextFile("./.env", envContent);

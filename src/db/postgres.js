import dotenv from "dotenv";
dotenv.config();

import { Pool } from "pg";

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

export const connectToDb = async () => {
  const client = await pool.connect();
  try {
    await client.query("SELECT 1");
    console.log("PostgresSQL connected");
  } catch (error) {
    client.release();
  }
};

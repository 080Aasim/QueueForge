import "dotenv/config";
import { createClient } from "redis";

export const redis = createClient({
  url: process.env.REDIS_URL || "redis://localhost:6379",
});

// Dedicated connection for blocking BRPOP commands
export const blockingRedis = redis.duplicate();

redis.on("error", (error) => {
  console.error("Redis Error:", error);
});

blockingRedis.on("error", (error) => {
  console.error("Blocking Redis Error:", error);
});

export const connectRedis = async () => {
  if (!redis.isOpen) {
    await redis.connect();
  }

  if (!blockingRedis.isOpen) {
    await blockingRedis.connect();
  }

  console.log("Redis connected");
};

export const disconnectRedis = async () => {
  if(blockingRedis.isOpen) {
    await blockingRedis.quit();
  }
  if(redis.isOpen) {
    await redis.quit();
  }
  console.log("Redis Connection closed");
  
}

import "dotenv/config";
import { defineConfig } from "prisma/config";

export default defineConfig({
  schema: "prisma/schema.prisma",
  migrations: {
    path: "prisma/migrations",
  },
  // The OWNER connection, used only for migrations. The running service uses
  // APP_DATABASE_URL and connects as a role that owns nothing, so RLS applies.
  datasource: {
    url: process.env["DATABASE_URL"],
  },
});

import type { ReactNode } from "react";
import { Ticket } from "lucide-react";
import { ThemeToggle } from "@/components/app/ThemeToggle";

/** The page around sign-in and the account pages: the tenant's name, a line, and a card. */
export function AccountFrame({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle: string;
  children: ReactNode;
}) {
  return (
    <div className="flex min-h-screen flex-col">
      <div className="flex justify-end p-3">
        <ThemeToggle />
      </div>
      <div className="flex flex-1 items-center justify-center px-4 pb-16">
        <div className="w-full max-w-sm">
          <div className="mb-8 text-center">
            <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-accent text-accent-foreground">
              <Ticket className="h-6 w-6" />
            </div>
            <h1 className="text-3xl font-semibold">{title}</h1>
            <p className="mt-1 text-sm text-muted-foreground">{subtitle}</p>
          </div>
          {children}
        </div>
      </div>
    </div>
  );
}

/** The card a form or a message sits in. */
export const accountCard = "space-y-4 rounded-xl border bg-card p-6 shadow-sm";

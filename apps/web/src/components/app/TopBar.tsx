import { Link, useNavigate } from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import { Ticket, User } from "lucide-react";
import { api, clearTokens, getSession, getTenantName } from "@/api";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { ThemeToggle } from "./ThemeToggle";

// Nav items. Organiser tools, Q&A and community slot in here later.
const NAV = [
  { to: "/", label: "Events" },
  { to: "/tickets", label: "My tickets" },
] as const;

const ROLE_LABEL = { ADMIN: "Admin", ORGANIZER: "Organiser", ATTENDEE: "Attendee" };

export function TopBar() {
  const session = getSession();
  const navigate = useNavigate();
  const qc = useQueryClient();

  // Forget the session HERE first, then tell Auth. Waiting on /auth/logout before clearing
  // left the tokens in this browser for the next person if the tab closed, or Auth was slow.
  const signOut = async () => {
    const refreshToken = getSession()?.tokens.refreshToken;
    await qc.cancelQueries();
    clearTokens();
    qc.clear();
    navigate({ to: "/login", search: { redirect: "/" }, replace: true });
    if (refreshToken) void api().logout(refreshToken);
  };

  return (
    <header className="sticky top-0 z-30 border-b bg-background/85 backdrop-blur">
      <div className="mx-auto flex h-14 max-w-5xl items-center gap-2 px-4">
        <Link to="/" className="mr-2 flex items-center gap-2 font-display text-lg font-semibold">
          <Ticket className="h-5 w-5 text-primary" />
          <span>{getTenantName()}</span>
        </Link>
        <nav className="flex items-center gap-1 text-sm">
          {NAV.map((n) => (
            <Link
              key={n.to}
              to={n.to}
              activeOptions={{ exact: n.to === "/" }}
              className="rounded-md px-2.5 py-1.5 text-muted-foreground transition-colors hover:text-foreground data-[status=active]:bg-secondary data-[status=active]:text-foreground"
            >
              {n.label}
            </Link>
          ))}
        </nav>
        <div className="ml-auto flex items-center gap-1">
          <ThemeToggle />
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="icon" aria-label="Account menu">
                <User className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuLabel className="font-normal">
                <div className="text-xs text-muted-foreground">Signed in as</div>
                <div>{session ? ROLE_LABEL[session.claims.role] : "—"}</div>
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem onSelect={signOut}>Sign out</DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>
    </header>
  );
}

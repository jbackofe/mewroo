import { Outlet, NavLink } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { ModeToggle } from "@/components/mode-toggle";

function NavItem({ to, label }: { to: string; label: string }) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        [
          "text-sm",
          "px-3 py-2 rounded-md",
          isActive
            ? "bg-muted text-foreground"
            : "text-muted-foreground hover:text-foreground hover:bg-muted",
        ].join(" ")
      }
      end={to === "/"}
    >
      {label}
    </NavLink>
  );
}

export default function RootLayout() {
  return (
    <div className="min-h-screen">
      <header className="border-b">
        <div className="mx-auto max-w-5xl px-6 py-4 flex items-center justify-between">
          <div className="font-semibold">mewroo</div>

          <nav className="flex items-center gap-1">
            <NavItem to="/" label="Home" />
            <NavItem to="/docs" label="Docs" />
            <NavItem to="/about" label="About" />
          </nav>

          <div className="flex items-center gap-2">
            <ModeToggle />
            <Button variant="outline">Sign in</Button>
            <Button>Get started</Button>
          </div>
        </div>
      </header>

      <Outlet />
    </div>
  );
}

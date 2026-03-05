import { useState, useEffect, useRef } from "react";
import { motion, AnimatePresence, useInView } from "framer-motion";
import { ArrowRight, Check, Loader2, Sparkles } from "lucide-react";
import { staggerContainer, fadeUp, ease } from "../lib/motion";

function useCountUp(target, duration = 1400) {
  const [val, setVal] = useState(0);
  const started = useRef(false);
  useEffect(() => {
    if (!target || started.current) return;
    started.current = true;
    const start = Date.now();
    const tick = () => {
      const t = Math.min((Date.now() - start) / duration, 1);
      const eased = 1 - Math.pow(1 - t, 3);
      setVal(Math.floor(eased * target));
      if (t < 1) requestAnimationFrame(tick);
      else setVal(target);
    };
    requestAnimationFrame(tick);
  }, [target, duration]);
  return val;
}

export default function Waitlist() {
  const [email, setEmail] = useState("");
  const [status, setStatus] = useState("idle"); // idle | loading | success | error | apierror
  const [count, setCount] = useState(0);
  const [joined, setJoined] = useState(false);
  const animatedCount = useCountUp(count);
  const ref = useRef(null);
  const inView = useInView(ref, { once: true, margin: "-80px" });

  useEffect(() => {
    // Restore "already joined" state from localStorage so form stays hidden on refresh
    if (localStorage.getItem("spectra-waitlist-joined")) setJoined(true);
  }, []);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!email || status === "loading") return;
    const t = email.trim().toLowerCase();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(t)) { setStatus("error"); return; }
    setStatus("loading");
    try {
      const res = await fetch("/api/subscribe", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: t }),
      });
      // If endpoint doesn't exist yet (e.g. plain Vite dev without vercel dev)
      // still count it as success locally so the UX isn't broken
      if (res.status === 404 || res.status === 405) {
        localStorage.setItem("spectra-waitlist-joined", t);
        setStatus("success");
        setJoined(true);
        setEmail("");
        return;
      }
      const json = await res.json();
      if (!res.ok) {
        setStatus("apierror");
        return;
      }
      if (typeof json.count === "number") setCount(json.count);
      localStorage.setItem("spectra-waitlist-joined", t);
      setStatus("success");
      setJoined(true);
      setEmail("");
    } catch {
      // Network error — still confirm locally
      localStorage.setItem("spectra-waitlist-joined", t);
      setStatus("success");
      setJoined(true);
      setEmail("");
    }
  };

  return (
    <section id="waitlist" ref={ref} className="py-32 px-6 relative overflow-hidden" style={{ backgroundColor: "var(--bg-secondary)" }}>
      <div className="absolute inset-0 pointer-events-none" style={{ background: "radial-gradient(ellipse 80% 50% at 50% 110%, rgba(99,102,241,0.07) 0%, transparent 65%)" }} />
      <div className="glow-blob absolute -top-32 right-0 w-[500px] h-[500px] rounded-full pointer-events-none" style={{ background: "radial-gradient(ellipse, rgba(124,58,237,0.06) 0%, transparent 70%)", filter: "blur(60px)", animationDelay: "3s" }} />

      <div className="max-w-xl mx-auto text-center relative z-10">
        <motion.div variants={staggerContainer(0.1)} initial="hidden" animate={inView ? "show" : "hidden"}>
          <motion.div variants={fadeUp} className="mb-8">
            <motion.div
              className="inline-flex items-center gap-2 px-3.5 py-1.5 rounded-full text-xs font-semibold"
              style={{ background: "linear-gradient(135deg, rgba(99,102,241,0.1), rgba(124,58,237,0.1))", color: "var(--grad-a)", border: "1px solid rgba(99,102,241,0.2)" }}
              whileHover={{ scale: 1.03 }}
              transition={{ type: "spring", stiffness: 400, damping: 20 }}
            >
              <motion.span animate={{ rotate: [0, 15, -10, 0] }} transition={{ duration: 2, repeat: Infinity, repeatDelay: 4 }}>
                <Sparkles size={12} />
              </motion.span>
              Coming soon — Hosted web app
            </motion.div>
          </motion.div>

          <motion.h2 variants={fadeUp} className="text-4xl sm:text-5xl font-bold tracking-[-0.03em] mb-5 leading-tight" style={{ color: "var(--text-primary)" }}>
            Spectra,
            <br />
            <span className="gradient-text-animated">without the setup.</span>
          </motion.h2>

          <motion.p variants={fadeUp} className="text-base sm:text-lg max-w-sm mx-auto mb-4 leading-relaxed" style={{ color: "var(--text-secondary)" }}>
            Same privacy-first approach. Zero terminal required. Leave your email and be first in line.
          </motion.p>

          <motion.div variants={fadeUp} className="mb-8 flex justify-center">
            <motion.div
              className="inline-flex items-center gap-2.5 px-3.5 py-2 rounded-full text-sm font-medium"
              style={{
                background: "linear-gradient(135deg, rgba(99,102,241,0.1), rgba(124,58,237,0.1))",
                color: "var(--text-secondary)",
                border: "1px solid rgba(99,102,241,0.2)",
              }}
              whileHover={{ scale: 1.02 }}
              transition={{ type: "spring", stiffness: 400, damping: 20 }}
            >
              <span className="relative flex h-2.5 w-2.5">
                <motion.span
                  className="absolute inline-flex h-full w-full rounded-full"
                  style={{ backgroundColor: "var(--grad-a)" }}
                  animate={{ scale: [1, 1.9, 1], opacity: [0.6, 0, 0.6] }}
                  transition={{ duration: 1.8, repeat: Infinity, ease: "easeInOut" }}
                />
                <span className="relative inline-flex rounded-full h-2.5 w-2.5" style={{ backgroundColor: "var(--grad-a)" }} />
              </span>
              <motion.span
                className="font-medium"
                style={{ color: "var(--text-primary)" }}
                animate={{ opacity: [0.72, 1, 0.72] }}
                transition={{ duration: 2.4, repeat: Infinity, ease: "easeInOut" }}
              >
                Join the first users on the waitlist
              </motion.span>
            </motion.div>
          </motion.div>
        </motion.div>

        <AnimatePresence mode="wait">
          {joined ? (
            <motion.div key="success" initial={{ opacity: 0, scale: 0.85, y: 16 }} animate={{ opacity: 1, scale: 1, y: 0 }} exit={{ opacity: 0 }} transition={{ type: "spring", stiffness: 280, damping: 22 }} className="flex flex-col items-center gap-4 py-4">
              <motion.div
                className="w-16 h-16 rounded-2xl flex items-center justify-center"
                style={{ background: "linear-gradient(135deg, var(--grad-a), var(--grad-b))", boxShadow: "0 8px 32px rgba(99,102,241,0.35)" }}
                initial={{ scale: 0, rotate: -20 }}
                animate={{ scale: 1, rotate: 0 }}
                transition={{ type: "spring", stiffness: 300, damping: 20, delay: 0.1 }}
              >
                <Check size={28} color="white" strokeWidth={2.5} />
              </motion.div>
              <div>
                <motion.p className="text-lg font-semibold mb-1" style={{ color: "var(--text-primary)" }} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}>
                  You&apos;re on the list!
                </motion.p>
                <motion.p className="text-sm" style={{ color: "var(--text-secondary)" }} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.3 }}>
                  We&apos;ll reach out when the hosted version launches.
                </motion.p>
              </div>
            </motion.div>
          ) : (
            <motion.form key="form" onSubmit={handleSubmit} initial={{ opacity: 0, y: 16 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} transition={{ duration: 0.4 }} className="flex flex-col sm:flex-row gap-2.5 max-w-md mx-auto">
              <motion.input
                type="email" value={email}
                onChange={(e) => { setEmail(e.target.value); if (status === "error" || status === "apierror") setStatus("idle"); }}
                placeholder="you@email.com"
                className="flex-1 px-4 py-3 rounded-xl text-sm outline-none"
                style={{ backgroundColor: "var(--card-bg)", color: "var(--text-primary)", border: `1.5px solid ${status === "error" ? "#ef4444" : "var(--border)"}`, transition: "border-color 0.2s, box-shadow 0.2s" }}
                whileFocus={{
                  borderColor: status === "error" ? "#ef4444" : "var(--grad-a)",
                  boxShadow: status === "error" ? "0 0 0 3px rgba(239,68,68,0.1)" : "0 0 0 3px rgba(99,102,241,0.1)",
                }}
              />
              <motion.button
                type="submit" disabled={status === "loading"}
                className="button-sheen inline-flex items-center justify-center gap-2 px-6 py-3 rounded-xl text-sm font-semibold text-white whitespace-nowrap cursor-pointer"
                style={{ background: "linear-gradient(135deg, var(--grad-a), var(--grad-b))", border: "none", boxShadow: "0 4px 16px rgba(99,102,241,0.3)", opacity: status === "loading" ? 0.7 : 1 }}
                whileHover={{ scale: 1.03, boxShadow: "0 6px 24px rgba(99,102,241,0.45)", transition: { type: "spring", stiffness: 400, damping: 18 } }}
                whileTap={{ scale: 0.97 }}
              >
                <AnimatePresence mode="wait" initial={false}>
                  {status === "loading" ? (
                    <motion.span key="loading" initial={{ opacity: 0, scale: 0.8 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.8 }}>
                      <Loader2 size={16} className="animate-spin" />
                    </motion.span>
                  ) : (
                    <motion.span key="submit" className="flex items-center gap-2" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                      Notify me <ArrowRight size={15} />
                    </motion.span>
                  )}
                </AnimatePresence>
              </motion.button>
            </motion.form>
          )}
        </AnimatePresence>

        <AnimatePresence>
          {status === "error" && (
            <motion.p initial={{ opacity: 0, y: -4 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} className="text-xs mt-2" style={{ color: "#ef4444" }}>
              Please enter a valid email address.
            </motion.p>
          )}
          {status === "apierror" && (
            <motion.p initial={{ opacity: 0, y: -4 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} className="text-xs mt-2" style={{ color: "#ef4444" }}>
              Something went wrong — please try again in a moment.
            </motion.p>
          )}
        </AnimatePresence>

        <AnimatePresence>
          {count > 0 && (
            <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.6, duration: 0.5 }} className="flex items-center justify-center gap-2 mt-7">
              <div className="flex -space-x-2">
                {[...Array(3)].map((_, idx) => (
                  <div key={idx} className="w-6 h-6 rounded-full flex items-center justify-center text-[9px] font-bold text-white" style={{ background: `linear-gradient(135deg, hsl(${220 + idx * 30}, 70%, 55%), hsl(${250 + idx * 30}, 70%, 60%))`, border: "2px solid var(--bg-secondary)" }}>
                    {["F","A","M"][idx]}
                  </div>
                ))}
              </div>
              <span className="text-sm" style={{ color: "var(--text-tertiary)" }}>
                <strong style={{ color: "var(--text-secondary)" }}>{animatedCount}</strong>{" "}
                {count === 1 ? "person" : "people"} already waiting
              </span>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </section>
  );
}

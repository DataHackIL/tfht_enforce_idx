import { getAccessEmail, parseAllowedEmails, jsonResponse } from "../_shared.js";

export async function onRequestGet({ request, env }) {
  const email = getAccessEmail(request, env);
  const allowed = parseAllowedEmails(env);
  const isAllowed = Boolean(email) && (allowed.length === 0 || allowed.includes(email.toLowerCase()));
  return jsonResponse({ email, allowed: isAllowed });
}

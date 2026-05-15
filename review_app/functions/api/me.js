import { getAccessEmail, jsonResponse, parseAllowedEmails } from "../_shared.js";

export async function onRequestGet({ request, env }) {
  const email = getAccessEmail(request, env);
  const allowedEmails = parseAllowedEmails(env);
  const allowed = Boolean(email) && (allowedEmails.length === 0 || allowedEmails.includes(email.toLowerCase()));
  return jsonResponse({ email, allowed });
}

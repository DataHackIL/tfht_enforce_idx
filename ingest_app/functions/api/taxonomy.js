import { jsonResponse, TAXONOMY } from "../_shared.js";

export async function onRequestGet() {
  return jsonResponse(TAXONOMY);
}

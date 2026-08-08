package api

import (
	"encoding/json"
	"net/http"
)

type errorEnvelope struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func respondJSON(w http.ResponseWriter, status int, body any) {
	payload, err := json.Marshal(body)
	if err != nil {
		status = http.StatusInternalServerError
		payload = []byte(`{"error":{"code":"internal_error","message":"failed to encode response"}}`)
	}
	payload = append(payload, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

func respondError(w http.ResponseWriter, status int, code string, message string) {
	respondJSON(w, status, errorEnvelope{Error: apiError{Code: code, Message: message}})
}

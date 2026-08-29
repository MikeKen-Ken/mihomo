package route

import (
	"encoding/json"

	"github.com/metacubex/mihomo/networkrecovery"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func networkRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/status", getNetworkRecoveryStatus)
	r.Post("/recover", recoverNetwork)
	return r
}

func getNetworkRecoveryStatus(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, networkrecovery.Status())
}

func recoverNetwork(w http.ResponseWriter, r *http.Request) {
	request := networkrecovery.Request{}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	switch request.Kind {
	case networkrecovery.KindDNSChanged,
		networkrecovery.KindDNSFailure,
		networkrecovery.KindRouteChanged,
		networkrecovery.KindEscalated:
	default:
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("unsupported recovery kind"))
		return
	}

	render.JSON(w, r, networkrecovery.Recover(request))
}

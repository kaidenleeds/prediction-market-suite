package server

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestR159PolyUSNoVenueResponseAlwaysOverridesAttemptedFalse(t *testing.T) {
	rec := httptest.NewRecorder()
	writePolyUSLiveNoVenueResponse(rec, http.StatusConflict, map[string]any{
		"error":               "pre-submit refusal",
		"risk_reservation_id": "risk-1",
		// Even a mistaken caller value cannot turn a proven no-send into an attempted order.
		"venue_attempted": true,
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusConflict)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if attempted, ok := got["venue_attempted"].(bool); !ok || attempted {
		t.Fatalf("venue_attempted=%v (%T), want explicit false", got["venue_attempted"], got["venue_attempted"])
	}
	if got["error"] != "pre-submit refusal" || got["risk_reservation_id"] != "risk-1" {
		t.Fatalf("response evidence was not preserved: %+v", got)
	}
}

func TestR159PolyUSHandlerClassifiesEveryResponseAroundVenuePOST(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "server.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var handler *ast.FuncDecl
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "handlePolyUSLiveOrder" {
			handler = fn
			break
		}
	}
	if handler == nil {
		t.Fatal("handlePolyUSLiveOrder not found")
	}

	var post token.Pos
	ast.Inspect(handler.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok &&
			selector.Sel.Name == "PlaceLiveOrder" {
			post = call.Pos()
		}
		return true
	})
	if !post.IsValid() {
		t.Fatal("PolyUS PlaceLiveOrder boundary not found")
	}

	preSubmitClassified, postSubmitClassified := 0, 0
	ast.Inspect(handler.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		switch ident.Name {
		case "writeJSON":
			if call.Pos() < post {
				t.Errorf("raw writeJSON remains before PlaceLiveOrder at %s; proven no-send responses must use the typed helper",
					fset.Position(call.Pos()))
				return true
			}
			snippet := string(src[fset.Position(call.Pos()).Offset:fset.Position(call.End()).Offset])
			if !strings.Contains(snippet, `"venue_attempted"`) ||
				!strings.Contains(snippet, "true") {
				t.Errorf("post-submit response lacks explicit venue_attempted=true at %s: %s",
					fset.Position(call.Pos()), snippet)
			}
			postSubmitClassified++
		case "writePolyUSLiveNoVenueResponse":
			if call.Pos() > post {
				t.Errorf("no-venue response helper used after PlaceLiveOrder at %s",
					fset.Position(call.Pos()))
				return true
			}
			preSubmitClassified++
		}
		return true
	})
	if preSubmitClassified == 0 || postSubmitClassified == 0 {
		t.Fatalf("response-boundary coverage missing: pre=%d post=%d",
			preSubmitClassified, postSubmitClassified)
	}
}

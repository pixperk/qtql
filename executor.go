package main

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"
)

// QueryPlan represents a parsed query before building the operator tree
// we want to apply limits after filters to match SQL semantics
// Order: KVScan → Filters → Limit → Project
type QueryPlan struct {
	Filters []FilterClause
	Limit   int  // 0 means no limit
	KeyOnly bool // SELECT key (default false = return both)
}

type FilterClause struct {
	Field    string // "KEY" or "VALUE"
	Operator string // "LIKE" or "CONTAINS"
	Operand  string // the pattern or substring
}

// simple query planner that parses a limited SQL-like syntax
func ParseQuery(parts []string) (*QueryPlan, error) {
	plan := &QueryPlan{
		Filters: []FilterClause{},
		Limit:   0,
		KeyOnly: false,
	}

	for i := 1; i < len(parts); i++ {
		part := strings.ToUpper(parts[i])

		switch part {
		case "SELECT":
			if i+1 >= len(parts) {
				return nil, errors.New("SELECT requires column (key or *)")
			}
			col := strings.ToUpper(parts[i+1])

			switch col {
			case "KEY":
				plan.KeyOnly = true
			case "*", "KEY,VALUE", "VALUE,KEY":
				plan.KeyOnly = false // both (default)
			default:
				return nil, errors.New("SELECT must be: key or * (for both)")
			}
			i++

		case "LIMIT":
			if i+1 >= len(parts) {
				return nil, errors.New("LIMIT requires a number")
			}
			n, err := strconv.Atoi(parts[i+1])
			if err != nil {
				return nil, errors.New("invalid LIMIT value: " + parts[i+1])
			}
			plan.Limit = n
			i++

		case "WHERE":
			if i+3 >= len(parts) {
				return nil, errors.New("WHERE requires: field operator value")
			}
			field := strings.ToUpper(parts[i+1])
			operator := strings.ToUpper(parts[i+2])
			operand := parts[i+3]

			// Validate the clause
			if field != "KEY" && field != "VALUE" {
				return nil, errors.New("WHERE field must be KEY or VALUE, got: " + field)
			}
			if field == "KEY" && operator != "LIKE" {
				return nil, errors.New("KEY only supports LIKE operator")
			}
			if field == "VALUE" && operator != "CONTAINS" {
				return nil, errors.New("VALUE only supports CONTAINS operator")
			}

			plan.Filters = append(plan.Filters, FilterClause{
				Field:    field,
				Operator: operator,
				Operand:  operand,
			})
			i += 3
		}
	}

	return plan, nil
}

// BuildOperatorTree constructs the operator tree from a QueryPlan
// Order: KVScan then Filters then Limit then Project (SQL semantics)
func BuildOperatorTree(store *Store, plan *QueryPlan) Operator {
	var op Operator = NewKVScan(store)

	// Apply filters first
	for _, f := range plan.Filters {
		filter := f // capture for closure
		switch {
		case filter.Field == "KEY" && filter.Operator == "LIKE":
			op = &Filter{
				Input: op,
				Pred: func(r Row) bool {
					matched, _ := filepath.Match(filter.Operand, r.Key.name)
					return matched
				},
			}
		case filter.Field == "VALUE" && filter.Operator == "CONTAINS":
			op = &Filter{
				Input: op,
				Pred: func(r Row) bool {
					return strings.Contains(r.Value.data, filter.Operand)
				},
			}
		}
	}

	// Apply limit
	if plan.Limit > 0 {
		op = &Limit{Input: op, Max: plan.Limit}
	}

	// Apply projection last
	if plan.KeyOnly {
		op = &Project{Input: op, KeyOnly: true}
	}

	return op
}

// PrintQueryPlan prints a visual representation of the query plan
func PrintQueryPlan(plan *QueryPlan) string {
	var sb strings.Builder

	sb.WriteString("Query Plan:\n")
	sb.WriteString("───────────\n")

	// Build from top to bottom (execution order is bottom-up)
	indent := 0

	// Projection at top (executes last)
	if plan.KeyOnly {
		sb.WriteString(strings.Repeat("  ", indent))
		sb.WriteString("→ Project (key only)\n")
		indent++
	}

	if plan.Limit > 0 {
		sb.WriteString(strings.Repeat("  ", indent))
		sb.WriteString("→ Limit (max=" + strconv.Itoa(plan.Limit) + ")\n")
		indent++
	}

	for i := len(plan.Filters) - 1; i >= 0; i-- {
		f := plan.Filters[i]
		sb.WriteString(strings.Repeat("  ", indent))
		sb.WriteString("→ Filter (" + f.Field + " " + f.Operator + " \"" + f.Operand + "\")\n")
		indent++
	}

	sb.WriteString(strings.Repeat("  ", indent))
	sb.WriteString("→ KVScan\n")

	return sb.String()
}

// ExecuteQuery runs the operator tree and collects results
func ExecuteQuery(op Operator) ([]*Row, error) {
	if err := op.Open(); err != nil {
		return nil, err
	}

	defer op.Close()

	var results []*Row

	for {
		row, err := op.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			break
		}
		results = append(results, row)
	}

	return results, nil
}

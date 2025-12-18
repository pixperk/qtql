package main

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

package stack

import "fmt"

type Stack []string

func Push(stk *Stack, val string) *Stack {
	if stk == nil {
		fmt.Printf("Error: Stack is nil!\n")
		return nil
	}
	if stk == nil {
		fmt.Printf("Error : *Stack is nil!\n")
		return nil
	}
	(*stk) = append(*stk, val)
	return stk
}

func Pop(stk *Stack) (string, *Stack) {
	if stk == nil || len(*stk) == 0 {
		return "", nil
	}
	n := len(*stk) - 1
	ret := ((*stk)[:n])
	return (*stk)[n], &ret
}

func Merge(dest, src *Stack) *Stack {
	for _, str := range *src {
		Push(dest, str)
	}
	return dest
}

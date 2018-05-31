// Package brokerutil provides a common interface to message-brokers for pub-sub
// applications.
//
// Use brokerutil to be able to build pub-sub applications which are not
// highly dependent on the message-brokers drivers implementation.
// brokerutil provides a common interface which enables developers to switch
// the message broker without having to rewrite major parts of the applications
// pub-sub logic.
package brokerutil

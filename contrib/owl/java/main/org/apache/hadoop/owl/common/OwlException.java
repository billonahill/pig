/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.owl.common;

import java.io.IOException;

/**
 * Class representing exceptions thrown by the metadata APIs.
 */
@SuppressWarnings("serial")
public class OwlException extends IOException {

    /** The error type enum for this exception. */
    private ErrorType errorType;

    /**
     * Instantiates a new owl exception.
     * 
     * @param errorType
     *            the error type
     */
    public OwlException(ErrorType errorType) {
        this(errorType, (String) null);
    }

    /**
     * Instantiates a new owl exception.
     * 
     * @param errorType
     *            the error type
     * @param extraMessage
     *            extra messages to add to the message string
     */
    public OwlException(ErrorType errorType, String extraMessage) {
        super(buildErrorMessage(
                errorType,
                extraMessage,
                null));
        this.errorType = errorType;
    }

    /**
     * Instantiates a new owl exception.
     * 
     * @param errorType
     *            the error type
     * @param cause
     *            the cause
     */
    public OwlException(ErrorType errorType, Throwable cause) {
        this(errorType, null, cause);
    }

    /**
     * Instantiates a new owl exception.
     * 
     * @param errorType
     *            the error type
     * @param extraMessage
     *            extra messages to add to the message string
     * @param cause
     *            the cause
     */
    public OwlException(ErrorType errorType, String extraMessage, Throwable cause) {
        super(buildErrorMessage(
                errorType,
                extraMessage,
                cause), cause);
        this.errorType = errorType;
    }


    /**
     * Instantiates a new owl exception. This private constructor is for use only if the complete
     * error message is being passed, nothing needs to be added. This is used from the owl client.
     * The other constructors are the preferred constructors. 
     * 
     * @param errorMessage
     *            the error message
     * @param errorType
     *            the error type
     */
    private OwlException(String errorMessage, ErrorType errorType) {
        super(errorMessage);
        this.errorType = errorType;
    }


    /**
     * Static builder for client exception construction.
     * 
     * @param errorMessage
     *            the error message
     * @param errorType
     *            the error type
     * 
     * @return the created owl exception
     */
    public static OwlException createException(String errorMessage, ErrorType errorType) {
        return new OwlException(errorMessage, errorType);
    }

    /**
     * Builds the error message string. The error type message is appended with the extra message. If appendCause
     * is true for the error type, then the message of the cause also is added to the message.
     * 
     * @param type
     *            the error type
     * @param extraMessage
     *            the extra message string
     * @param cause
     *            the cause for the exception
     * 
     * @return the exception message string
     */
    public static String buildErrorMessage(ErrorType type, String extraMessage, Throwable cause) {

        //Initial message is just the error type message
        StringBuffer message = new StringBuffer(OwlException.class.getName());
        message.append(" : " + type.getErrorCode());
        message.append(" : " + type.getErrorMessage());

        if( extraMessage != null ) {
            //Add the extra message value to buffer
            message.append(" : " + extraMessage);
        }

        if( type.appendCauseMessage() ) {
            if( cause != null && cause.getMessage() != null ) {
                //Add the cause message to buffer
                message.append(". Cause : " + cause.getMessage());
            }
        }

        return message.toString();
    }


    /**
     * Is this a retriable error.
     * 
     * @return is it retriable
     */
    public boolean isRetriable() {
        return errorType.isRetriable();
    }

    /**
     * Gets the error type.
     * 
     * @return the error type enum
     */
    public ErrorType getErrorType() {
        return errorType;
    }

    /**
     * Gets the error code.
     * 
     * @return the error code
     */
    public int getErrorCode() {

        if( errorType == null ) {
            return -1;
        }

        return errorType.getErrorCode();
    }

    /* (non-Javadoc)
     * @see java.lang.Throwable#toString()
     */
    @Override
    public String toString() {
        return getMessage();
    }

}

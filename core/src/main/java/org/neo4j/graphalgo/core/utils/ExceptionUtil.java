/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.utils;

import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;

import java.lang.reflect.InvocationTargetException;

public final class ExceptionUtil {
    private static final String UNEXPECTED_MESSAGE = "Unexpected Exception";

    // from org.neo4j.helpers.ExceptionUtil.chain() 3.5
    
    // todo - try this:
//    use throw e or throw new RuntimeException(e) directly. 
//    Prefer multi-caches if applicable. For more elaborate scenarios, have a look at throwIfUnchecked(Throwable) and throwIfInstanceOf(Throwable, Class)
//    For a more furrow explanation take a look at the very similar case: Why we deprecated Throwables.propagate        
    
    public static RuntimeException launderedException(Class<RuntimeException> type, String messageForUnexpected, Throwable exception )
    {
        if ( type.isInstance( exception ) )
        {
            return type.cast( exception );
        }
        else if ( exception instanceof Error )
        {
            throw (Error) exception;
        }
        else if ( exception instanceof InvocationTargetException)
        {
            return launderedException( type, messageForUnexpected,
                    ( (InvocationTargetException) exception ).getTargetException() );
        }
        else if ( exception instanceof RuntimeException )
        {
            throw (RuntimeException) exception;
        }
        else
        {
            throw new RuntimeException( messageForUnexpected, exception );
        }
    }

    public static RuntimeException launderedException(String message, Throwable exception) {
        return launderedException(RuntimeException.class, message, exception);
    }
    
    public static RuntimeException launderedException(Throwable exception ) {
        return launderedException(RuntimeException.class, UNEXPECTED_MESSAGE, exception);
    }
        
    
    
    // from org.neo4j.helpers.ExceptionUtil.chain() 3.5
    // todo - try doing Optional.ofNullable
    public static <T extends Throwable> T chain(T initial, T current) {
        if ( initial == null ) {
            return current;
        }
        if (current != null) {
            initial.addSuppressed( current );
        }
        return initial;
    }

    public static RuntimeException asUnchecked(final Throwable exception) {
        if (exception instanceof RuntimeException) {
            return (RuntimeException) exception;
        }
        if (exception instanceof Error) {
            throw (Error) exception;
        }
        return new RuntimeException(exception);
    }

    public static <T> T throwKernelException(KernelException e) {
        Status status = e.status();
        String codeString = status.code().serialize();
        String message = e.getMessage();
        String newMessage;
        if (message == null || message.isEmpty()) {
            newMessage = codeString;
        } else {
            newMessage = codeString + ": " + message;
        }
        throw new RuntimeException(newMessage, e);
    }

    private ExceptionUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}

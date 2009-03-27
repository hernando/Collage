
/* Copyright (c) 2005-2008, Stefan Eilemann <eile@equalizergraphics.com> 
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *  
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#ifndef EQNET_CONNECTIONDESCRIPTION_H
#define EQNET_CONNECTIONDESCRIPTION_H

#include <eq/net/connectionType.h>

#include <eq/base/base.h>
#include <eq/base/referenced.h>

namespace eq
{
namespace net
{
    /**
     * Describes the connection to a Node.
     *
     * @sa Node
     */
    class EQ_EXPORT ConnectionDescription : public base::Referenced
    {
    public:
        ConnectionDescription() 
                : type( CONNECTIONTYPE_TCPIP )
                , bandwidth( 0 )
                , launchTimeout( 10000 )
                , launchCommandQuote( '\'' )
            {
                TCPIP.port = 0;
            }

        /** The network protocol for the connection. */
        ConnectionType type;

        ///** The bandwidth in kilobyte per second for this connection. */
        int32_t bandwidth;

        /** 
         * The amount of time in milliseconds to wait before a node is
         * considered unreachable during start.
         */
        int32_t launchTimeout;

        /** The individual parameters for the connection. */
        union
        {
            /** TCP/IP parameters */
            struct
            {
                /** The listening port. */
                uint16_t port;

            } TCPIP, SDP;
        };

        /** The character to quote the launch command arguments */
        char launchCommandQuote;

        /** @return this description as a string. */
        std::string toString() const;
        void serialize( std::ostream& os ) const;

        /** 
         * Reads the connection description from a string.
         * 
         * The string is consumed as the description is parsed.
         *
         * @param data the string containing the connection description.
         * @return <code>true</code> if the information was read correctly, 
         *         <code>false</code> if not.
         */
        bool fromString( std::string& data );

        /** @name Data Access
         *
         * std::strings are not public because of DLL allocation issues.
         */
        //*{
        void setHostname( const std::string& hostname );
        const std::string& getHostname() const;
        void setLaunchCommand( const std::string& launchCommand );
        const std::string& getLaunchCommand() const;
        //*}
    protected:
        virtual ~ConnectionDescription() {}

    private:
        /** 
         * The command to spawn a new process on the node, e.g., 
         * "ssh eile@node1".
         * 
         * %h - hostname
         * %c - command
         * %n - unique node identifier
         */
        std::string _launchCommand; 

        /** The host name. */
        std::string _hostname;
    };

    std::ostream& operator << ( std::ostream&, const ConnectionDescription* );
}
}

#endif // EQNET_CONNECTION_DESCRIPTION_H


/* Copyright (c) 2005-2014, Carlos Duelo <cduelo@cesvima.upm.es>
 *
 * This file is part of Collage <https://github.com/Eyescale/Collage>
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License version 2.1 as published
 * by the Free Software Foundation.
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

#ifndef CO_MPICONNECTION_H
#define CO_MPICONNECTION_H

#include <co/connection.h>

#include <lunchbox/thread.h>

#include <mpi.h>

#include <map>

namespace co
{
namespace detail { class MPIConnection; } 

/** MPI connection  */
class MPIConnection : public Connection
{
    public:
        /** Construct a new MPI connection. */
        CO_API MPIConnection();

        /** Destruct this MPI connection. */
        CO_API ~MPIConnection();

        virtual bool connect();
		virtual bool listen();
        virtual void close();

		virtual void acceptNB();
		virtual ConnectionPtr acceptSync();

		virtual Notifier getNotifier() const { return _notifier; }

    protected:
		void readNB( void* buffer, const uint64_t bytes );
		int64_t readSync( void* buffer, const uint64_t bytes, const bool ignored);
		int64_t write( const void* buffer, const uint64_t bytes );

	private:
		Notifier	_notifier;
		
		detail::MPIConnection * const _impl;
};

}
#endif //CO_MPICONNECTION_H

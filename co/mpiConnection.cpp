
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

#include "mpiConnection.h"
#include "connectionDescription.h"

namespace co
{

namespace detail
{
	class AsyncConnection : lunchbox::Thread
	{
		public:

			AsyncConnection(int * peerRank, int rank, char * mpiPort, MPI_Comm * interComm, MPI_Request * request)
			{
				_peerRank = peerRank;
				_rank = rank;
				_mpiPort = mpiPort;
				_interComm = interComm;	
				_status = true;
				_request = request;

				start();
			}

			bool wait()
			{
				join();
				return _status;
			}

			void run()
			{
				if (MPI_SUCCESS != MPI_Wait(_request, NULL))
				{
					LBERROR << "Could not start accepting a MPI connection."<< std::endl;
					_status = false;
					return;
				}
				delete _request;

				std::cout<<"SERVER RECIBIDO RANK "<<*_peerRank<<std::endl;

				if (MPI_SUCCESS != MPI_Open_port(MPI_INFO_NULL, _mpiPort))
				{
					LBERROR << "Error accepting a MPI connection."<< std::endl;
					_status = false;
					return;
				}

				std::cout<<"SERVER CREADO PUERTO "<<_mpiPort<<std::endl;

				if (MPI_SUCCESS != MPI_Send(_mpiPort, MPI_MAX_PORT_NAME, MPI_CHAR, *_peerRank, 0, MPI_COMM_WORLD))
				{
					LBERROR << "Error accepting a MPI connection."<< std::endl;
					_status = false;
					return;
				}
				std::cout<<"SERVER MANDADO PUERTO "<<std::endl;

				if ( MPI_SUCCESS != MPI_Comm_accept(_mpiPort, MPI_INFO_NULL, _rank, MPI_COMM_SELF, _interComm))
				{
					LBERROR << "Error accepting a MPI connection."<< std::endl;
					_status = false;
					return;
				}
				std::cout<<"SERVER ACEPTADO CLIENTE"<<std::endl;

				if (MPI_SUCCESS != MPI_Close_port(_mpiPort))
				{
					LBERROR << "Error accepting a MPI connection."<< std::endl;
					_status = false;
					return;
				}
			}

		private:
			int			_rank;
			int *		_peerRank;
			char * 		_mpiPort;
			MPI_Comm *  _interComm;

			bool		_status;

			MPI_Request * _request;
	};

	class MPIConnection
	{
		public:
			MPIConnection() : 
							_rank(-1),
							_peerRank(-1),
							_asyncConnection(0)
			{
			}

			int			_rank;
			int			_peerRank;

			std::map<void *, MPI_Request>	_requests;

			char		_mpiPort[MPI_MAX_PORT_NAME];
			MPI_Comm	_interComm;
			
			AsyncConnection * _asyncConnection;
	};
}


MPIConnection::MPIConnection() : 
					_notifier(-1),
					_impl(new detail::MPIConnection)
{
	ConnectionDescriptionPtr description = _getDescription();
	description->type = CONNECTIONTYPE_MPI;
    description->bandwidth = 1024000; // For example :S

	// Ask rank of the process
	if (MPI_SUCCESS != MPI_Comm_rank(MPI_COMM_WORLD, &_impl->_rank))
	{
        LBERROR << "Could not determine the rank of the calling process in the communicator: MPI_COMM_WORLD" << std::endl;
	}

    LBASSERT( _impl->_rank >= 0 );
}

MPIConnection::~MPIConnection()
{
	delete _impl;
}

bool MPIConnection::connect()
{
    LBASSERT( getDescription()->type == CONNECTIONTYPE_MPI );

    if( !isClosed() )
        return false;

    _setState( STATE_CONNECTING );

	ConnectionDescriptionPtr description = _getDescription();
	_impl->_peerRank = description->port;

	std::cout<<"CLIENTE CONECTANDO CON "<<_impl->_peerRank<<std::endl;

	if (MPI_SUCCESS != MPI_Send(&_impl->_rank, 1, MPI_INT, _impl->_peerRank, 0, MPI_COMM_WORLD))
	{
        LBERROR << "Could not connect to "<< _impl->_peerRank << " process."<< std::endl;
		return false;
	}

	std::cout<<"CLIENTE RANK SENDED"<<std::endl;
	if (MPI_SUCCESS != MPI_Recv(_impl->_mpiPort, MPI_MAX_PORT_NAME, MPI_CHAR, _impl->_peerRank, 0, MPI_COMM_WORLD, NULL))
	{
        LBERROR << "Could not connect to "<< _impl->_peerRank << " process."<< std::endl;
		return false;
	}

	std::cout<<"CLIENTE "<<_impl->_mpiPort<<std::endl;

	sleep(4);

	if (MPI_SUCCESS != MPI_Comm_connect(_impl->_mpiPort, MPI_INFO_NULL, _impl->_peerRank, MPI_COMM_SELF, &_impl->_interComm))
	{
        LBERROR << "Could not connect to "<< _impl->_peerRank << " process."<< std::endl;
		return false;
	}

	std::cout<<"CLIENTE CONNECTED"<<std::endl;

    _setState( STATE_CONNECTED );

    LBINFO << "Connected " << description->toString() << std::endl;

	return true;
}

bool MPIConnection::listen()
{
    if( !isClosed( ))
        return false;

    _setState( STATE_LISTENING );

	return true;
}

void MPIConnection::close()
{
    if( isClosed( ))
        return;

	MPI_Comm_disconnect(&_impl->_interComm);

    _setState( STATE_CLOSED );
}

void MPIConnection::acceptNB()
{

	MPI_Request * request = new MPI_Request;

	std::cout<<"SERVER SOY "<<_impl->_rank<<std::endl;

	// Recieve the peer rank
	if (MPI_SUCCESS != MPI_Irecv(&_impl->_peerRank, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, request))
	{
		LBERROR << "Could not start accepting a MPI connection."<< std::endl;
		return;
	}

	LBASSERT( _impl->_asyncConnection == 0 )

	_impl->_asyncConnection = new detail::AsyncConnection(&_impl->_peerRank, _impl->_rank, _impl->_mpiPort, &_impl->_interComm, request);
}

ConnectionPtr MPIConnection::acceptSync()
{
    if( !isListening( ))
        return 0;
	
	if (!_impl->_asyncConnection->wait())
	{
		LBERROR << "Error accepting a MPI connection."<< std::endl;
		return 0;
	}
	
	delete _impl->_asyncConnection;
	_impl->_asyncConnection = 0;

    LBASSERT( _impl->_peerRank >= 0 );

	std::cout<<"ACEPPPPPPPPPPPPPPPPPPPPPP " <<_impl->_peerRank<<std::endl;



#if 0
    ConstConnectionDescriptionPtr description = getDescription();
    MPIConnection* newConnection = new MPIConnection( );
    newConnection->_setState( STATE_CONNECTED );
    ConnectionPtr connection( newConnection ); // to keep ref-counting correct
#endif
	return 0;
}

void MPIConnection::readNB( void* buffer, const uint64_t bytes )
{
	buffer = 0;
	if (bytes && buffer)
		return;
}

int64_t MPIConnection::readSync( void* buffer, const uint64_t bytes, const bool block )
{
	buffer = 0;
	if (block  && buffer)
		return 0;
	return bytes;
}

int64_t MPIConnection::write( const void* buffer, const uint64_t bytes )
{
	buffer = 0;
	if (buffer)
		return bytes;
	return 0;
}

}

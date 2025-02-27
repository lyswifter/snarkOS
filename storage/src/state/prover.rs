// Copyright (C) 2019-2022 Aleo Systems Inc.
// This file is part of the snarkOS library.

// The snarkOS library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// The snarkOS library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with the snarkOS library. If not, see <https://www.gnu.org/licenses/>.

use crate::storage::{DataMap, MapId, MapRead, MapReadWrite, Storage, StorageAccess, StorageReadWrite};
use snarkvm::dpc::prelude::*;

use anyhow::{anyhow, Result};
use std::path::Path;

#[derive(Debug)]
pub struct ProverState<N: Network, A: StorageAccess> {
    /// The coinbase records of the prover in storage.
    coinbase: CoinbaseState<N, A>,
}

impl<N: Network, A: StorageAccess> ProverState<N, A> {
    /// Opens a new instance of `ProverState` from the given storage path.
    pub fn open<S: Storage<Access = A>, P: AsRef<Path>>(path: P) -> Result<Self> {
        // Open storage.
        let context = N::NETWORK_ID;
        let storage = S::open(path, context)?;

        // Initialize the prover.
        let prover = Self {
            coinbase: CoinbaseState::open(storage)?,
        };

        info!("Prover successfully initialized");
        Ok(prover)
    }

    /// Returns `true` if the given commitment exists in storage.
    pub fn contains_coinbase_record(&self, commitment: &N::Commitment) -> Result<bool> {
        self.coinbase.contains_record(commitment)
    }

    /// Returns all coinbase records in storage.
    pub fn to_coinbase_records(&self) -> Vec<(u32, Record<N>)> {
        self.coinbase.to_records()
    }

    /// Returns the coinbase record for a given commitment.
    pub fn get_coinbase_record(&self, commitment: &N::Commitment) -> Result<(u32, Record<N>)> {
        self.coinbase.get_record(commitment)
    }
}

impl<N: Network, A: StorageReadWrite> ProverState<N, A> {
    /// Adds the given coinbase record to storage.
    pub fn add_coinbase_record(&self, block_height: u32, record: Record<N>) -> Result<()> {
        self.coinbase.add_record(block_height, record)
    }

    /// Removes the given record from storage.
    pub fn remove_coinbase_record(&self, commitment: &N::Commitment) -> Result<()> {
        self.coinbase.remove_record(commitment)
    }
}

#[derive(Clone, Debug)]
#[allow(clippy::type_complexity)]
struct CoinbaseState<N: Network, A: StorageAccess> {
    records: DataMap<N::Commitment, (u32, Record<N>), A>,
}

impl<N: Network, A: StorageAccess> CoinbaseState<N, A> {
    /// Initializes a new instance of `CoinbaseState`.
    fn open<S: Storage<Access = A>>(storage: S) -> Result<Self> {
        Ok(Self {
            records: storage.open_map(MapId::Records)?,
        })
    }

    /// Returns `true` if the given commitment exists in storage.
    fn contains_record(&self, commitment: &N::Commitment) -> Result<bool> {
        self.records.contains_key(commitment)
    }

    /// Returns all records in storage.
    fn to_records(&self) -> Vec<(u32, Record<N>)> {
        self.records.values().collect()
    }

    /// Returns the record for a given commitment.
    fn get_record(&self, commitment: &N::Commitment) -> Result<(u32, Record<N>)> {
        match self.records.get(commitment)? {
            Some((block_height, record)) => Ok((block_height, record)),
            None => Err(anyhow!("Record with commitment {} does not exist in storage", commitment)),
        }
    }
}

impl<N: Network, A: StorageReadWrite> CoinbaseState<N, A> {
    /// Adds the given block height and record to storage.
    fn add_record(&self, block_height: u32, record: Record<N>) -> Result<()> {
        // Ensure the record does not exist.
        let commitment = record.commitment();
        if self.records.contains_key(&commitment)? {
            Err(anyhow!("Record with commitment {} already exists in storage", commitment))
        } else {
            // Insert the record.
            self.records.insert(&commitment, &(block_height, record), None)?;
            Ok(())
        }
    }

    /// Removes the given record from storage.
    fn remove_record(&self, commitment: &N::Commitment) -> Result<()> {
        // Remove the record entry.
        self.records.remove(commitment, None)?;
        Ok(())
    }
}

use crate::streaming::event_parser::common::EventMetadata;
use crate::streaming::event_parser::core::account_event_parser::{
    NonceAccountEvent, TokenAccountEvent, TokenInfoEvent,
};
use crate::streaming::event_parser::protocols::block::block_meta_event::BlockMetaEvent;
use crate::streaming::event_parser::protocols::bonk::events::*;
use crate::streaming::event_parser::protocols::pumpfun::events::*;
use crate::streaming::event_parser::protocols::pumpswap::events::*;
use crate::streaming::event_parser::protocols::raydium_amm_v4::events::*;
use crate::streaming::event_parser::protocols::raydium_clmm::events::*;
use crate::streaming::event_parser::protocols::raydium_cpmm::events::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Unified Event Enum - Replaces the trait-based approach with a type-safe enum
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum UnifiedEvent {
    // Bonk events
    BonkTradeEvent(BonkTradeEvent),
    BonkPoolCreateEvent(BonkPoolCreateEvent),
    BonkMigrateToAmmEvent(BonkMigrateToAmmEvent),
    BonkMigrateToCpswapEvent(BonkMigrateToCpswapEvent),
    BonkPoolStateAccountEvent(BonkPoolStateAccountEvent),
    BonkGlobalConfigAccountEvent(BonkGlobalConfigAccountEvent),
    BonkPlatformConfigAccountEvent(BonkPlatformConfigAccountEvent),

    // PumpFun events
    PumpFunCreateTokenEvent(PumpFunCreateTokenEvent),
    PumpFunTradeEvent(PumpFunTradeEvent),
    PumpFunMigrateEvent(PumpFunMigrateEvent),
    PumpFunBondingCurveAccountEvent(PumpFunBondingCurveAccountEvent),
    PumpFunGlobalAccountEvent(PumpFunGlobalAccountEvent),

    // PumpSwap events
    PumpSwapBuyEvent(PumpSwapBuyEvent),
    PumpSwapSellEvent(PumpSwapSellEvent),
    PumpSwapCreatePoolEvent(PumpSwapCreatePoolEvent),
    PumpSwapDepositEvent(PumpSwapDepositEvent),
    PumpSwapWithdrawEvent(PumpSwapWithdrawEvent),
    PumpSwapGlobalConfigAccountEvent(PumpSwapGlobalConfigAccountEvent),
    PumpSwapPoolAccountEvent(PumpSwapPoolAccountEvent),

    // Raydium AMM V4 events
    RaydiumAmmV4SwapEvent(RaydiumAmmV4SwapEvent),
    RaydiumAmmV4DepositEvent(RaydiumAmmV4DepositEvent),
    RaydiumAmmV4WithdrawEvent(RaydiumAmmV4WithdrawEvent),
    RaydiumAmmV4WithdrawPnlEvent(RaydiumAmmV4WithdrawPnlEvent),
    RaydiumAmmV4Initialize2Event(RaydiumAmmV4Initialize2Event),
    RaydiumAmmV4AmmInfoAccountEvent(RaydiumAmmV4AmmInfoAccountEvent),

    // Raydium CLMM events
    RaydiumClmmSwapEvent(RaydiumClmmSwapEvent),
    RaydiumClmmSwapV2Event(RaydiumClmmSwapV2Event),
    RaydiumClmmClosePositionEvent(RaydiumClmmClosePositionEvent),
    RaydiumClmmIncreaseLiquidityV2Event(RaydiumClmmIncreaseLiquidityV2Event),
    RaydiumClmmDecreaseLiquidityV2Event(RaydiumClmmDecreaseLiquidityV2Event),
    RaydiumClmmCreatePoolEvent(RaydiumClmmCreatePoolEvent),
    RaydiumClmmOpenPositionWithToken22NftEvent(RaydiumClmmOpenPositionWithToken22NftEvent),
    RaydiumClmmOpenPositionV2Event(RaydiumClmmOpenPositionV2Event),
    RaydiumClmmAmmConfigAccountEvent(RaydiumClmmAmmConfigAccountEvent),
    RaydiumClmmPoolStateAccountEvent(RaydiumClmmPoolStateAccountEvent),
    RaydiumClmmTickArrayStateAccountEvent(RaydiumClmmTickArrayStateAccountEvent),

    // Raydium CPMM events
    RaydiumCpmmSwapEvent(RaydiumCpmmSwapEvent),
    RaydiumCpmmDepositEvent(RaydiumCpmmDepositEvent),
    RaydiumCpmmWithdrawEvent(RaydiumCpmmWithdrawEvent),
    RaydiumCpmmInitializeEvent(RaydiumCpmmInitializeEvent),
    RaydiumCpmmAmmConfigAccountEvent(RaydiumCpmmAmmConfigAccountEvent),
    RaydiumCpmmPoolStateAccountEvent(RaydiumCpmmPoolStateAccountEvent),

    // Common events
    TokenAccountEvent(TokenAccountEvent),
    NonceAccountEvent(NonceAccountEvent),
    TokenInfoEvent(TokenInfoEvent),
    BlockMetaEvent(BlockMetaEvent),
}

impl UnifiedEvent {
    pub fn metadata(&self) -> &EventMetadata {
        match self {
            UnifiedEvent::BonkTradeEvent(e) => &e.metadata,
            UnifiedEvent::BonkPoolCreateEvent(e) => &e.metadata,
            UnifiedEvent::BonkMigrateToAmmEvent(e) => &e.metadata,
            UnifiedEvent::BonkMigrateToCpswapEvent(e) => &e.metadata,
            UnifiedEvent::BonkPoolStateAccountEvent(e) => &e.metadata,
            UnifiedEvent::BonkGlobalConfigAccountEvent(e) => &e.metadata,
            UnifiedEvent::BonkPlatformConfigAccountEvent(e) => &e.metadata,
            UnifiedEvent::PumpFunCreateTokenEvent(e) => &e.metadata,
            UnifiedEvent::PumpFunTradeEvent(e) => &e.metadata,
            UnifiedEvent::PumpFunMigrateEvent(e) => &e.metadata,
            UnifiedEvent::PumpFunBondingCurveAccountEvent(e) => &e.metadata,
            UnifiedEvent::PumpFunGlobalAccountEvent(e) => &e.metadata,
            UnifiedEvent::PumpSwapBuyEvent(e) => &e.metadata,
            UnifiedEvent::PumpSwapSellEvent(e) => &e.metadata,
            UnifiedEvent::PumpSwapCreatePoolEvent(e) => &e.metadata,
            UnifiedEvent::PumpSwapDepositEvent(e) => &e.metadata,
            UnifiedEvent::PumpSwapWithdrawEvent(e) => &e.metadata,
            UnifiedEvent::PumpSwapGlobalConfigAccountEvent(e) => &e.metadata,
            UnifiedEvent::PumpSwapPoolAccountEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumAmmV4SwapEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumAmmV4DepositEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumAmmV4WithdrawEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumAmmV4WithdrawPnlEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumAmmV4Initialize2Event(e) => &e.metadata,
            UnifiedEvent::RaydiumAmmV4AmmInfoAccountEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumClmmSwapEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumClmmSwapV2Event(e) => &e.metadata,
            UnifiedEvent::RaydiumClmmClosePositionEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumClmmIncreaseLiquidityV2Event(e) => &e.metadata,
            UnifiedEvent::RaydiumClmmDecreaseLiquidityV2Event(e) => &e.metadata,
            UnifiedEvent::RaydiumClmmCreatePoolEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumClmmOpenPositionWithToken22NftEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumClmmOpenPositionV2Event(e) => &e.metadata,
            UnifiedEvent::RaydiumClmmAmmConfigAccountEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumClmmPoolStateAccountEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumClmmTickArrayStateAccountEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumCpmmSwapEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumCpmmDepositEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumCpmmWithdrawEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumCpmmInitializeEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumCpmmAmmConfigAccountEvent(e) => &e.metadata,
            UnifiedEvent::RaydiumCpmmPoolStateAccountEvent(e) => &e.metadata,
            UnifiedEvent::TokenAccountEvent(e) => &e.metadata,
            UnifiedEvent::NonceAccountEvent(e) => &e.metadata,
            UnifiedEvent::TokenInfoEvent(e) => &e.metadata,
            UnifiedEvent::BlockMetaEvent(e) => &e.metadata,
        }
    }

    pub fn metadata_mut(&mut self) -> &mut EventMetadata {
        match self {
            UnifiedEvent::BonkTradeEvent(e) => &mut e.metadata,
            UnifiedEvent::BonkPoolCreateEvent(e) => &mut e.metadata,
            UnifiedEvent::BonkMigrateToAmmEvent(e) => &mut e.metadata,
            UnifiedEvent::BonkMigrateToCpswapEvent(e) => &mut e.metadata,
            UnifiedEvent::BonkPoolStateAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::BonkGlobalConfigAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::BonkPlatformConfigAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::PumpFunCreateTokenEvent(e) => &mut e.metadata,
            UnifiedEvent::PumpFunTradeEvent(e) => &mut e.metadata,
            UnifiedEvent::PumpFunMigrateEvent(e) => &mut e.metadata,
            UnifiedEvent::PumpFunBondingCurveAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::PumpFunGlobalAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::PumpSwapBuyEvent(e) => &mut e.metadata,
            UnifiedEvent::PumpSwapSellEvent(e) => &mut e.metadata,
            UnifiedEvent::PumpSwapCreatePoolEvent(e) => &mut e.metadata,
            UnifiedEvent::PumpSwapDepositEvent(e) => &mut e.metadata,
            UnifiedEvent::PumpSwapWithdrawEvent(e) => &mut e.metadata,
            UnifiedEvent::PumpSwapGlobalConfigAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::PumpSwapPoolAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumAmmV4SwapEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumAmmV4DepositEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumAmmV4WithdrawEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumAmmV4WithdrawPnlEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumAmmV4Initialize2Event(e) => &mut e.metadata,
            UnifiedEvent::RaydiumAmmV4AmmInfoAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumClmmSwapEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumClmmSwapV2Event(e) => &mut e.metadata,
            UnifiedEvent::RaydiumClmmClosePositionEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumClmmIncreaseLiquidityV2Event(e) => &mut e.metadata,
            UnifiedEvent::RaydiumClmmDecreaseLiquidityV2Event(e) => &mut e.metadata,
            UnifiedEvent::RaydiumClmmCreatePoolEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumClmmOpenPositionWithToken22NftEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumClmmOpenPositionV2Event(e) => &mut e.metadata,
            UnifiedEvent::RaydiumClmmAmmConfigAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumClmmPoolStateAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumClmmTickArrayStateAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumCpmmSwapEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumCpmmDepositEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumCpmmWithdrawEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumCpmmInitializeEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumCpmmAmmConfigAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::RaydiumCpmmPoolStateAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::TokenAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::NonceAccountEvent(e) => &mut e.metadata,
            UnifiedEvent::TokenInfoEvent(e) => &mut e.metadata,
            UnifiedEvent::BlockMetaEvent(e) => &mut e.metadata,
        }
    }
}

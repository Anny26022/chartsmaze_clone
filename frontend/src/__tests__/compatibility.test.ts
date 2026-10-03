import { describe, expect, it } from 'vitest';
import { SCANNER_IDENTITY, assertScannerIdentity } from '../engine/compatibility';

describe('scanner compatibility', () => {
  it('accepts the exact deployed identity', () => {
    expect(() => assertScannerIdentity({
      engineVersion: 'sha256:8013351008f2c6a09792a073d8f4246ba910b2f937d966a628a21ea5a5f14306',
      conditionContractHash: 'c1a0e79f4396c02532874aa8a91c07fd28e83fd102599cdf8788ec36f4b92b85',
    })).not.toThrow();
  });
  it.each([null, {}, {...SCANNER_IDENTITY, engineVersion:'old'},
    {...SCANNER_IDENTITY, conditionContractHash:'old'}])('rejects absent or different identities', value => {
    expect(() => assertScannerIdentity(value)).toThrow('incompatible');
  });
});

// Jest setup: Mock ESM-only packages that can't be compiled by ts-jest

// Mock @nodable/entities which is ESM-only and causes issues with ts-jest
jest.mock('@nodable/entities', () => ({
  EntityDecoder: class EntityDecoder {},
  EntityParser: class EntityParser {},
  XMLParser: class XMLParser {},
  HTMLParser: class HTMLParser {},
}), { virtual: true });

jest.mock('@nodable/entities/src/index.js', () => ({
  default: { EntityDecoder: class EntityDecoder {} },
  EntityDecoder: class EntityDecoder {},
}), { virtual: true });

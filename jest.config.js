module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/src', '<rootDir>/lib', '<rootDir>/docker', '<rootDir>/test'],
  testMatch: ['**/*.test.ts'],
  setupFilesAfterEnv: ['<rootDir>/jest.setup.js'],
  moduleNameMapper: {
    '^integration-huron-person$': '<rootDir>/node_modules/integration-huron-person/dist/esm/bin/index.js',
    '^integration-huron-person/(.*)$': '<rootDir>/node_modules/integration-huron-person/dist/types/$1'
  },
  transform: {
    '^.+\\.tsx?$': 'ts-jest'
  },
  transformIgnorePatterns: ['node_modules/']
};

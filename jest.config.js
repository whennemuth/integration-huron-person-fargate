module.exports = {
  testEnvironment: 'node',
  roots: ['<rootDir>/test'],
  testMatch: ['**/*.test.ts'],
  setupFilesAfterEnv: ['<rootDir>/jest.setup.js'],
  moduleNameMapper: {
    '^integration-huron-person$': '<rootDir>/node_modules/integration-huron-person/dist/esm/bin/index.js',
    '^integration-huron-person/(.*)$': '<rootDir>/node_modules/integration-huron-person/dist/types/$1'
  },
  transform: {
    '^.+\\.tsx?$': ['ts-jest', {
      isolatedModules: true,
      tsconfig: {
        module: 'commonjs',
        esModuleInterop: true
      }
    }]
  },
  transformIgnorePatterns: ['node_modules/']
};

module.exports = {
  docs: [
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      items: [
        'intro',
        'install',
        'examples'
      ],
    },
    {
      type: 'category',
      label: 'How to use Gatekeeper',
      collapsed: false,
      items: [
        'howto',
        'audit',
        'violations',
        'sync',
        'exempt-namespaces',
        'library',
        'customize-startup',
        'customize-admission',
        'debug',
        'emergency',
        'vendor-specific'
      ],
    },
    {
      type: 'category',
      label: 'Contributing',
      collapsed: false,
      items: [
        'help',
        'security'
      ],
    }
  ]
};
